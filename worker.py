"""
worker.py
Per-account Colab worker process.

Each worker manages one Chrome window with 3 Colab tabs for a single
Google account.  It writes a cell, runs it, monitors for bot_blocked,
and restarts all tabs when 2+ are blocked.
"""

import functools
import logging
import os
import platform
import shutil
import socket
import subprocess
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException,
    NoSuchWindowException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException,
    JavascriptException,
    TimeoutException,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
COLAB_URL = "https://colab.research.google.com/#create=true"
NUM_TABS = 3
MONITOR_INTERVAL = 15          # seconds between output checks
BOT_BLOCKED_TAB_THRESHOLD = 1  # bot_blocked count for a tab to be "blocked"
BLOCKED_TABS_TO_RESTART = 2    # how many tabs must be blocked to trigger restart
RESTART_DELAY = 30             # seconds to wait after teardown before re-running

CELL_CONTENT = (
    "!pip install -q ytminer-client coolname\n"
    "\n"
    "import coolname\n"
    "worker = coolname.generate_slug(2)\n"
    "\n"
    "!ytminer-download \\\n"
    "    --server http://obelix.cs.uiowa.edu:8765 \\\n"
    "    --output /tmp/videos \\\n"
    "    --upload \\\n"
    "    --delay 20 \\\n"
    "    --jitter 10 \\\n"
    "    --worker-name {worker}"
)

CHROME_FLAGS = [
    "--disable-gpu",
    "--disable-extensions",
    "--no-first-run",
    "--disable-default-apps",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows",
    "--disable-hang-monitor",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-component-update",
]


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class ChromeDisconnectedError(Exception):
    """Chrome process died or became unreachable."""


# ---------------------------------------------------------------------------
# Retry decorator for flaky Selenium DOM calls
# ---------------------------------------------------------------------------
def retry_dom(max_attempts=3, delay=1):
    """Retry on transient Selenium exceptions."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (StaleElementReferenceException,
                        ElementClickInterceptedException) as exc:
                    last_exc = exc
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                except NoSuchWindowException:
                    raise
                except WebDriverException as exc:
                    msg = str(exc).lower()
                    if "disconnected" in msg or "not reachable" in msg:
                        raise ChromeDisconnectedError(str(exc)) from exc
                    last_exc = exc
                    if attempt < max_attempts - 1:
                        time.sleep(delay * 2)
            raise last_exc
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logger(username):
    logger = logging.getLogger(f"colab.{username}")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        f"[%(asctime)s] [{username}] [%(levelname)-5s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, f"{username}.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Chrome binary detection (cross-platform)
# ---------------------------------------------------------------------------
def find_chrome_binary():
    system = platform.system()
    if system == "Darwin":
        candidates = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
    elif system == "Linux":
        candidates = ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium"]
    elif system == "Windows":
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        ]
    else:
        candidates = []

    for c in candidates:
        if os.path.isfile(c) or shutil.which(c):
            return c
    return None


# ---------------------------------------------------------------------------
# Chrome lifecycle
# ---------------------------------------------------------------------------
def launch_chrome(port, profile_dir, logger):
    """Launch Chrome with remote debugging.  Returns Popen handle."""
    chrome_path = find_chrome_binary()
    if not chrome_path:
        logger.error("Chrome not found. Install Chrome or set PATH.")
        return None

    os.makedirs(profile_dir, exist_ok=True)

    cmd = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ] + CHROME_FLAGS

    logger.info("Launching Chrome on port %d with profile %s", port, profile_dir)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Chrome binary not found at: %s", chrome_path)
        return None
    return proc


def wait_for_chrome(port, timeout=15.0, logger=None):
    """Block until Chrome's debug port is reachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(1)
    if logger:
        logger.error("Chrome not reachable on port %d after %.0fs", port, timeout)
    return False


def connect_to_chrome(port, logger):
    """Create Selenium WebDriver attached to Chrome on *port*."""
    opts = Options()
    opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    driver = webdriver.Chrome(options=opts, service=ChromeService())
    logger.info("Selenium connected on port %d", port)
    return driver


def kill_chrome(proc, logger, timeout=10):
    """Gracefully terminate Chrome, force-kill if needed."""
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
        logger.info("Chrome terminated gracefully")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        logger.warning("Chrome force-killed")
    except Exception as exc:
        logger.warning("Error killing Chrome: %s", exc)


# ---------------------------------------------------------------------------
# JS helpers
# ---------------------------------------------------------------------------
def js(driver, code):
    return driver.execute_script(code)


def wait_js(driver, code, timeout=60, poll=1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = js(driver, code)
            if result:
                return result
        except JavascriptException:
            pass
        time.sleep(poll)
    return None


# ---------------------------------------------------------------------------
# Sign-in
# ---------------------------------------------------------------------------
@retry_dom()
def sign_in(driver, username, password, logger):
    """Sign in to Google.  Returns True on success."""
    logger.info("Signing in ...")

    try:
        el = WebDriverWait(driver, 15).until(
            lambda d: d.find_element(By.CSS_SELECTOR, "input[type='email']")
        )
    except TimeoutException:
        logger.warning("Email field not found — may already be signed in")
        return "colab.research.google.com" in driver.current_url

    el.clear()
    el.send_keys(username)
    js(driver, """
        (document.querySelector('#identifierNext button') ||
         document.querySelector('#identifierNext')).click();
    """)
    logger.info("Email entered")
    time.sleep(4)

    try:
        el = WebDriverWait(driver, 30).until(
            lambda d: d.find_element(By.CSS_SELECTOR, "input[type='password']")
        )
    except TimeoutException:
        logger.warning("Password field not found — check for 2FA or CAPTCHA")
        return False

    el.clear()
    el.send_keys(password)
    js(driver, """
        (document.querySelector('#passwordNext button') ||
         document.querySelector('#passwordNext')).click();
    """)
    logger.info("Password entered")

    # Wait for redirect to Colab (handle 2FA / "Not now" prompts)
    deadline = time.time() + 300  # 5 minutes for manual 2FA
    warned_2fa = False
    while time.time() < deadline:
        url = driver.current_url
        if "colab.research.google.com" in url:
            logger.info("Sign-in complete")
            return True

        # Detect 2FA / challenge pages
        if not warned_2fa and "accounts.google.com" in url:
            page_text = js(driver, "return document.body.innerText || '';") or ""
            if any(kw in page_text.lower() for kw in [
                "2-step verification", "verify it", "confirm your identity",
                "security check", "enter the code",
            ]):
                logger.warning("2FA / security challenge detected — please complete manually")
                warned_2fa = True

        # Click "Not now" / "Skip" if present
        js(driver, """
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = (b.innerText || '').trim().toLowerCase();
                if (t === 'not now' || t === 'skip') { b.click(); break; }
            }
        """)
        time.sleep(3)

    logger.error("Sign-in timed out after 5 minutes")
    return False


def ensure_signed_in(driver, username, password, logger):
    """Navigate to Colab, handle sign-in if needed."""
    driver.get(COLAB_URL)
    time.sleep(5)

    if "accounts.google.com" in driver.current_url:
        if not sign_in(driver, username, password, logger):
            return False
        time.sleep(3)
        driver.get(COLAB_URL)
        time.sleep(5)

    # Handle Colab's sign-in dialog
    try:
        clicked = js(driver, """
            const btn = document.querySelector('mwc-dialog[open] md-text-button');
            if (btn) { btn.click(); return true; }
            return null;
        """)
        if clicked:
            logger.info("Clicked Colab sign-in dialog")
            time.sleep(5)
            if "accounts.google.com" in driver.current_url:
                if not sign_in(driver, username, password, logger):
                    return False
                time.sleep(3)
                driver.get(COLAB_URL)
                time.sleep(5)
    except (JavascriptException, WebDriverException):
        pass

    return True


# ---------------------------------------------------------------------------
# Tab management
# ---------------------------------------------------------------------------
def open_colab_tabs(driver, count, logger):
    """Open *count* Colab tabs (including the current one).  Returns handles."""
    handles = [driver.current_window_handle]
    for i in range(count - 1):
        driver.switch_to.new_window("tab")
        time.sleep(1)
        driver.get(COLAB_URL)
        handles.append(driver.current_window_handle)
        logger.info("Opened tab %d/%d", i + 2, count)
        time.sleep(3)
    return handles


# ---------------------------------------------------------------------------
# Cell writing & execution
# ---------------------------------------------------------------------------
@retry_dom()
def write_and_run_cell(driver, logger):
    """Write the single cell and run it.  Returns True on success."""
    logger.debug("Waiting for Monaco editor ...")
    result = wait_js(driver, """
        return window.monaco && window.monaco.editor.getEditors().length > 0;
    """, timeout=30)

    if not result:
        # Try clicking the first cell to activate the editor
        try:
            js(driver, "const c = document.querySelector('.cell'); if (c) c.click();")
        except JavascriptException:
            pass
        time.sleep(3)
        result = wait_js(driver, """
            return window.monaco && window.monaco.editor.getEditors().length > 0;
        """, timeout=15)

    if not result:
        logger.error("Monaco editor not found")
        return False

    # Close release notes if present
    try:
        js(driver, """
            const els = document.querySelectorAll('*');
            for (const el of els) {
                if ((el.innerText || '').trim() === 'close' && el.offsetParent !== null) {
                    el.click(); break;
                }
            }
        """)
    except JavascriptException:
        pass
    time.sleep(1)

    # Write cell content
    js(driver, f"window.monaco.editor.getEditors()[0].getModel().setValue({repr(CELL_CONTENT)});")
    logger.info("Cell written")

    # Run all
    try:
        js(driver, """
            const outer = document.querySelector('colab-notebook-toolbar-run-button');
            const inner = outer.shadowRoot.querySelector('#toolbar-run-button');
            inner.click();
        """)
    except (JavascriptException, WebDriverException) as exc:
        logger.warning("Run button click failed: %s — trying Ctrl+F9", exc)
        from selenium.webdriver.common.keys import Keys
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.CONTROL, Keys.F9)
    time.sleep(3)

    # Handle "Run anyway" dialog
    for _ in range(10):
        try:
            clicked = js(driver, """
                const dialogs = document.querySelectorAll('mwc-dialog[open]');
                for (const d of dialogs) {
                    const text = (d.textContent || '').toLowerCase();
                    if (text.includes('run anyway')) {
                        const allBtns = d.querySelectorAll('md-text-button, mwc-button, button');
                        for (const b of allBtns) {
                            if ((b.textContent || '').toLowerCase().includes('run anyway')) {
                                b.click();
                                return true;
                            }
                        }
                    }
                }
                return null;
            """)
            if clicked:
                logger.info("Clicked 'Run anyway'")
                break
        except JavascriptException:
            pass
        time.sleep(1)

    logger.info("Cell running")
    return True


# ---------------------------------------------------------------------------
# Output checking
# ---------------------------------------------------------------------------
@retry_dom()
def get_output_and_blocked_count(driver):
    """Scrape cell output, return (tail_text, bot_blocked_count)."""
    output = js(driver, """
        const cells = document.querySelectorAll('.cell');
        const result = [];
        for (let i = 0; i < cells.length; i++) {
            const out = cells[i].querySelector('.output_area, .output');
            result.push(out ? out.textContent : '');
        }
        return result.join('\\n');
    """) or ""
    blocked = output.lower().count("bot_blocked")
    lines = output.strip().split("\n")
    tail = " | ".join(lines[-2:])[:120]
    return tail, blocked


# ---------------------------------------------------------------------------
# Runtime teardown
# ---------------------------------------------------------------------------
@retry_dom()
def click_runtime_menu_item(driver, label, logger):
    """Open the Runtime menu and click an item by label text."""
    try:
        runtime_btn = driver.find_element(By.ID, "runtime-menu-button")
        runtime_btn.click()
        time.sleep(1)
        items = driver.find_elements(By.CSS_SELECTOR, ".goog-menuitem")
        for item in items:
            if label in item.text and item.is_displayed():
                ActionChains(driver).move_to_element(item).click().perform()
                return True
        # Close the menu if item not found
        driver.find_element(By.TAG_NAME, "body").click()
    except (NoSuchElementException, WebDriverException) as exc:
        logger.debug("click_runtime_menu_item(%s) failed: %s", label, exc)
    return False


def confirm_yes_dialog(driver, timeout=10):
    """Click 'Yes' in a confirmation dialog."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            clicked = js(driver, """
                const d = document.querySelector('mwc-dialog.yes-no-dialog[open]');
                if (!d) return null;
                const btns = d.querySelectorAll('md-text-button, mwc-button, button');
                for (const b of btns) {
                    if (b.textContent.trim() === 'Yes') { b.click(); return true; }
                }
                return null;
            """)
            if clicked:
                return True
        except JavascriptException:
            pass
        time.sleep(0.5)
    return False


def teardown_runtime(driver, tab_label, logger):
    """Tear down the Colab runtime on the current tab."""
    logger.info("[%s] Tearing down runtime ...", tab_label)
    click_runtime_menu_item(driver, "Interrupt execution", logger)
    time.sleep(3)
    click_runtime_menu_item(driver, "Disconnect and delete", logger)
    time.sleep(2)
    confirm_yes_dialog(driver)
    time.sleep(3)
    logger.info("[%s] Runtime deleted", tab_label)


# ---------------------------------------------------------------------------
# Worker entry point (called by multiprocessing.Process)
# ---------------------------------------------------------------------------
def run_worker(account_index, username, password, shutdown_event, status_dict):
    """Main worker function — one Chrome window, 3 Colab tabs."""
    port = 9222 + account_index
    base_dir = os.path.dirname(os.path.abspath(__file__))
    profile_dir = os.path.join(base_dir, "profiles", username)
    logger = setup_logger(username)

    logger.info("Worker starting (port=%d, profile=%s)", port, profile_dir)
    status_dict[username] = "STARTING"

    chrome_proc = None
    driver = None

    while not shutdown_event.is_set():
        try:
            # --- Phase 1: Launch Chrome ---
            status_dict[username] = "LAUNCHING_CHROME"
            chrome_proc = _launch_chrome_with_retry(port, profile_dir, logger)
            if chrome_proc is None:
                status_dict[username] = "CHROME_FAILED"
                logger.error("Cannot launch Chrome — retrying in 30s")
                _interruptible_sleep(30, shutdown_event)
                continue

            driver = connect_to_chrome(port, logger)

            # --- Phase 2: Sign in ---
            status_dict[username] = "SIGNING_IN"
            if not ensure_signed_in(driver, username, password, logger):
                status_dict[username] = "SIGNIN_FAILED"
                logger.error("Sign-in failed — retrying in 30s")
                _cleanup(driver, chrome_proc, logger)
                driver, chrome_proc = None, None
                _interruptible_sleep(30, shutdown_event)
                continue

            # --- Phase 3: Open tabs & run ---
            status_dict[username] = "SETTING_UP_TABS"
            tab_handles = open_colab_tabs(driver, NUM_TABS, logger)

            for i, handle in enumerate(tab_handles):
                driver.switch_to.window(handle)
                time.sleep(2)
                ok = write_and_run_cell(driver, logger)
                if not ok:
                    logger.warning("Tab %d setup failed — will try once more", i + 1)
                    time.sleep(3)
                    write_and_run_cell(driver, logger)

            # --- Phase 4: Monitor loop ---
            logger.info("All %d tabs running — entering monitor loop", NUM_TABS)
            _monitor_loop(driver, tab_handles, username, shutdown_event,
                          status_dict, logger)

        except ChromeDisconnectedError:
            logger.warning("Chrome disconnected — restarting in 5s")
            _cleanup(driver, chrome_proc, logger)
            driver, chrome_proc = None, None
            _interruptible_sleep(5, shutdown_event)

        except Exception as exc:
            logger.error("Unexpected error: %s", exc, exc_info=True)
            _cleanup(driver, chrome_proc, logger)
            driver, chrome_proc = None, None
            _interruptible_sleep(10, shutdown_event)

    # Shutdown
    logger.info("Shutdown signal received")
    _cleanup(driver, chrome_proc, logger)
    status_dict[username] = "STOPPED"
    logger.info("Worker stopped")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _launch_chrome_with_retry(port, profile_dir, logger, retries=3):
    """Try to launch Chrome up to *retries* times with exponential backoff."""
    for attempt in range(retries):
        proc = launch_chrome(port, profile_dir, logger)
        if proc is None:
            return None
        if wait_for_chrome(port, timeout=15, logger=logger):
            return proc
        # Chrome started but port not reachable — kill and retry
        logger.warning("Chrome port not reachable (attempt %d/%d)", attempt + 1, retries)
        kill_chrome(proc, logger)
        time.sleep(5 * (attempt + 1))
    return None


def _monitor_loop(driver, tab_handles, username, shutdown_event, status_dict, logger):
    """Monitor tabs for bot_blocked.  Returns when restart is needed or shutdown."""
    while not shutdown_event.is_set():
        blocked_counts = []
        valid_handles = []

        for i, handle in enumerate(tab_handles):
            try:
                driver.switch_to.window(handle)
                _, blocked = get_output_and_blocked_count(driver)
                blocked_counts.append(blocked)
                valid_handles.append(handle)
            except NoSuchWindowException:
                logger.warning("Tab %d disappeared — will re-open", i + 1)
                blocked_counts.append(-1)
            except ChromeDisconnectedError:
                raise
            except Exception as exc:
                logger.debug("Error checking tab %d: %s", i + 1, exc)
                blocked_counts.append(0)
                valid_handles.append(handle)

        # Re-open lost tabs
        lost = [i for i, c in enumerate(blocked_counts) if c == -1]
        if lost:
            tab_handles, blocked_counts = _recover_lost_tabs(
                driver, tab_handles, blocked_counts, lost, logger
            )

        # Update status
        status_dict[username] = f"RUNNING blocked={blocked_counts}"

        # Check bot_blocked threshold
        tabs_blocked = sum(
            1 for c in blocked_counts
            if c >= BOT_BLOCKED_TAB_THRESHOLD
        )
        if tabs_blocked >= BLOCKED_TABS_TO_RESTART:
            logger.warning(
                "%d/%d tabs blocked (counts=%s) — restarting all",
                tabs_blocked, len(tab_handles), blocked_counts,
            )
            _handle_bot_blocked(driver, tab_handles, shutdown_event,
                                status_dict, username, logger)

        _interruptible_sleep(MONITOR_INTERVAL, shutdown_event)


def _handle_bot_blocked(driver, tab_handles, shutdown_event, status_dict,
                        username, logger):
    """Teardown all runtimes, wait, re-run all cells."""
    status_dict[username] = "RESTARTING_TABS"

    # Teardown all
    for i, handle in enumerate(tab_handles):
        try:
            driver.switch_to.window(handle)
            teardown_runtime(driver, f"tab-{i+1}", logger)
        except NoSuchWindowException:
            logger.warning("Tab %d gone during teardown", i + 1)
        except Exception as exc:
            logger.warning("Teardown error on tab %d: %s", i + 1, exc)

    logger.info("Waiting %ds before re-running ...", RESTART_DELAY)
    _interruptible_sleep(RESTART_DELAY, shutdown_event)

    if shutdown_event.is_set():
        return

    # Re-run all
    for i, handle in enumerate(tab_handles):
        try:
            driver.switch_to.window(handle)
            time.sleep(2)
            ok = write_and_run_cell(driver, logger)
            if not ok:
                logger.warning("Re-run failed on tab %d", i + 1)
        except NoSuchWindowException:
            logger.warning("Tab %d gone during re-run", i + 1)
        except Exception as exc:
            logger.warning("Re-run error on tab %d: %s", i + 1, exc)

    logger.info("All tabs restarted")


def _recover_lost_tabs(driver, tab_handles, blocked_counts, lost_indices, logger):
    """Re-open lost tabs and set them up."""
    new_handles = list(tab_handles)
    new_counts = list(blocked_counts)

    for idx in lost_indices:
        try:
            # Switch to any surviving tab first
            surviving = [h for h in driver.window_handles if h in new_handles]
            if surviving:
                driver.switch_to.window(surviving[0])
            driver.switch_to.new_window("tab")
            time.sleep(1)
            driver.get(COLAB_URL)
            time.sleep(5)
            new_handle = driver.current_window_handle
            new_handles[idx] = new_handle
            new_counts[idx] = 0
            write_and_run_cell(driver, logger)
            logger.info("Recovered tab %d with new handle", idx + 1)
        except Exception as exc:
            logger.error("Failed to recover tab %d: %s", idx + 1, exc)

    return new_handles, new_counts


def _cleanup(driver, chrome_proc, logger):
    """Quit Selenium driver and kill Chrome."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    kill_chrome(chrome_proc, logger)


def _interruptible_sleep(seconds, shutdown_event):
    """Sleep in 1-second chunks, checking shutdown_event."""
    deadline = time.time() + seconds
    while time.time() < deadline and not shutdown_event.is_set():
        time.sleep(min(1, deadline - time.time()))
