"""
start_colab.py
Automated Google Colab: open multiple notebook tabs, sign in once,
write cells, run, and monitor for bot_blocked.

If bot_blocked > threshold on any tab, tear down that tab's runtime,
wait cooldown, and restart it.

Requires Chrome to be running with remote debugging:
  See README.md for setup instructions.
"""

import time
import sys
import os
import getpass
import platform
import subprocess
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
COLAB_URL = "https://colab.research.google.com/#create=true"
BOT_BLOCKED_THRESHOLD = 3
COOLDOWN_SECONDS = 5 * 60  # 5 minutes
NUM_TABS = 3
MONITOR_INTERVAL = 15  # seconds between output checks
DEBUG_PORT = 9222

CELL_1 = "!pip install -q ytminer-client"

CELL_2_TEMPLATE = (
    "!ytminer-download \\\n"
    "    --server http://obelix.cs.uiowa.edu:8765 \\\n"
    "    --output /tmp/videos \\\n"
    "    --upload \\\n"
    "    --delay 20 \\\n"
    "    --jitter 10 \\\n"
    "    --worker-name {worker_name}"
)

WORKER_NAMES = ["colabby-1", "colabby-2", "colabby-3"]


# ---------------------------------------------------------------------------
# Chrome launcher
# ---------------------------------------------------------------------------
def launch_chrome():
    """Launch Chrome with remote debugging if not already running."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(("127.0.0.1", DEBUG_PORT))
        s.close()
        print(f"[+] Chrome already running on port {DEBUG_PORT}")
        return
    except ConnectionRefusedError:
        pass

    print(f"[*] Launching Chrome with --remote-debugging-port={DEBUG_PORT} …")
    system = platform.system()
    if system == "Darwin":
        candidates = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
    elif system == "Linux":
        candidates = ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium"]
    elif system == "Windows":
        import glob
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        ]
        # Also check user-local install
        local = os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe")
        candidates.append(local)
    else:
        candidates = []

    chrome_path = None
    for c in candidates:
        if os.path.isfile(c) or shutil.which(c):
            chrome_path = c
            break

    if not chrome_path:
        print("[!] Could not find Chrome. Please launch it manually:")
        print(f"    chrome --remote-debugging-port={DEBUG_PORT} --user-data-dir=/tmp/colab_chrome_profile")
        print("    Then re-run this script.")
        sys.exit(1)

    # Use a temp dir for the profile (cross-platform)
    import tempfile as _tempfile
    profile_dir = os.path.join(_tempfile.gettempdir(), "colab_chrome_profile")

    try:
        subprocess.Popen(
            [chrome_path, f"--remote-debugging-port={DEBUG_PORT}", f"--user-data-dir={profile_dir}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        print(f"[!] Chrome not found at: {chrome_path}")
        print(f"    Please launch Chrome manually with --remote-debugging-port={DEBUG_PORT}")
        sys.exit(1)

    time.sleep(4)

    # Verify it's reachable
    try:
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.connect(("127.0.0.1", DEBUG_PORT))
        s2.close()
        print("[+] Chrome launched")
    except ConnectionRefusedError:
        print("[!] Chrome launched but port not reachable. Try:")
        print(f"    1. Close ALL Chrome windows first")
        print(f"    2. Re-run this script")
        print(f"    Or launch manually:")
        print(f'    "{chrome_path}" --remote-debugging-port={DEBUG_PORT} --user-data-dir={profile_dir}')
        sys.exit(1)


# ---------------------------------------------------------------------------
# Chrome connection
# ---------------------------------------------------------------------------
def connect():
    opts = Options()
    opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
    # Selenium 4.6+ auto-downloads the matching chromedriver
    return webdriver.Chrome(options=opts, service=ChromeService())


def js(driver, code):
    return driver.execute_script(code)


def wait_js(driver, code, timeout=60, poll=1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = js(driver, code)
        if result:
            return result
        time.sleep(poll)
    return None


# ---------------------------------------------------------------------------
# Sign in
# ---------------------------------------------------------------------------
def sign_in(driver, username, password):
    print("[*] Signing in …")

    el = WebDriverWait(driver, 15).until(
        lambda d: d.find_element(By.CSS_SELECTOR, "input[type='email']")
    )
    el.clear()
    el.send_keys(username)
    js(driver, """
        (document.querySelector('#identifierNext button') ||
         document.querySelector('#identifierNext')).click();
    """)
    print("[+] Email entered")
    time.sleep(4)

    el = WebDriverWait(driver, 30).until(
        lambda d: d.find_element(By.CSS_SELECTOR, "input[type='password']")
    )
    el.clear()
    el.send_keys(password)
    js(driver, """
        (document.querySelector('#passwordNext button') ||
         document.querySelector('#passwordNext')).click();
    """)
    print("[+] Password entered")

    deadline = time.time() + 60
    while time.time() < deadline:
        if "colab.research.google.com" in driver.current_url:
            print("[+] Sign-in complete")
            return
        js(driver, """
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const t = (b.innerText || '').trim().toLowerCase();
                if (t === 'not now' || t === 'skip') { b.click(); break; }
            }
        """)
        time.sleep(2)
    print("[~] Timed out waiting for Colab redirect")


# ---------------------------------------------------------------------------
# Tab management
# ---------------------------------------------------------------------------
def ensure_signed_in_first_tab(driver, username, password):
    driver.get(COLAB_URL)
    time.sleep(5)

    if "accounts.google.com" in driver.current_url:
        sign_in(driver, username, password)
        time.sleep(3)
        driver.get(COLAB_URL)
        time.sleep(5)

    clicked = js(driver, """
        const btn = document.querySelector('mwc-dialog[open] md-text-button');
        if (btn) { btn.click(); return true; }
        return null;
    """)
    if clicked:
        print("[+] Clicked sign-in dialog")
        time.sleep(5)
        if "accounts.google.com" in driver.current_url:
            sign_in(driver, username, password)
            time.sleep(3)
            driver.get(COLAB_URL)
            time.sleep(5)


def open_additional_tabs(driver, count):
    for i in range(count):
        driver.switch_to.new_window("tab")
        time.sleep(1)
        driver.get(COLAB_URL)
        print(f"[+] Opened tab {i + 2}")
        time.sleep(3)


def open_single_tab(driver, username, password):
    driver.switch_to.new_window("tab")
    time.sleep(1)
    driver.get(COLAB_URL)
    time.sleep(5)
    handle = driver.current_window_handle

    clicked = js(driver, """
        const btn = document.querySelector('mwc-dialog[open] md-text-button');
        if (btn) { btn.click(); return true; }
        return null;
    """)
    if clicked:
        time.sleep(5)
        if "accounts.google.com" in driver.current_url:
            sign_in(driver, username, password)
            time.sleep(3)
            driver.get(COLAB_URL)
            time.sleep(5)

    return handle


# ---------------------------------------------------------------------------
# Notebook setup & run (operates on current tab)
# ---------------------------------------------------------------------------
def setup_and_run(driver, worker_name):
    cell_2 = CELL_2_TEMPLATE.format(worker_name=worker_name)

    print(f"  [{worker_name}] Waiting for editor …")
    result = wait_js(driver, """
        return window.monaco && window.monaco.editor.getEditors().length > 0;
    """, timeout=30)
    if not result:
        js(driver, "const c = document.querySelector('.cell'); if (c) c.click();")
        time.sleep(3)
        result = wait_js(driver, """
            return window.monaco && window.monaco.editor.getEditors().length > 0;
        """, timeout=15)
    if not result:
        print(f"  [{worker_name}] Editor not found!")
        return False

    # Close release notes
    js(driver, """
        const els = document.querySelectorAll('*');
        for (const el of els) {
            if ((el.innerText || '').trim() === 'close' && el.offsetParent !== null) {
                el.click(); break;
            }
        }
    """)
    time.sleep(1)

    # Write cell 1
    js(driver, f"window.monaco.editor.getEditors()[0].getModel().setValue({repr(CELL_1)});")
    print(f"  [{worker_name}] Cell 1 written")

    # Add cell 2
    js(driver, """
        const buttons = document.querySelectorAll('colab-toolbar-button');
        for (const btn of buttons) {
            if (btn.shadowRoot) {
                const text = (btn.shadowRoot.textContent || '').trim();
                if (text.includes('Insert code cell')) {
                    const inner = btn.shadowRoot.querySelector('button') || btn;
                    inner.click();
                    break;
                }
            }
        }
    """)
    time.sleep(2)

    editors_count = js(driver, "return window.monaco.editor.getEditors().length;")
    if editors_count < 2:
        print(f"  [{worker_name}] Only {editors_count} editor(s)")
        return False

    js(driver, f"window.monaco.editor.getEditors()[1].getModel().setValue({repr(cell_2)});")
    print(f"  [{worker_name}] Cell 2 written")

    # Run all
    js(driver, """
        const outer = document.querySelector('colab-notebook-toolbar-run-button');
        const inner = outer.shadowRoot.querySelector('#toolbar-run-button');
        inner.click();
    """)
    time.sleep(3)

    # Handle "Run anyway"
    for _ in range(10):
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
            print(f"  [{worker_name}] Run anyway clicked")
            break
        time.sleep(1)

    print(f"  [{worker_name}] Running!")
    return True


# ---------------------------------------------------------------------------
# Runtime teardown (operates on current tab)
# ---------------------------------------------------------------------------
def click_runtime_menu_item(driver, label):
    runtime_btn = driver.find_element(By.ID, "runtime-menu-button")
    runtime_btn.click()
    time.sleep(1)
    items = driver.find_elements(By.CSS_SELECTOR, ".goog-menuitem")
    for item in items:
        if label in item.text and item.is_displayed():
            ActionChains(driver).move_to_element(item).click().perform()
            return True
    driver.find_element(By.TAG_NAME, "body").click()
    return False


def confirm_yes_dialog(driver, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
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
        time.sleep(0.5)
    return False


def teardown_runtime(driver, worker_name):
    print(f"  [{worker_name}] Tearing down runtime …")
    click_runtime_menu_item(driver, "Interrupt execution")
    time.sleep(3)
    click_runtime_menu_item(driver, "Disconnect and delete")
    time.sleep(2)
    confirm_yes_dialog(driver)
    time.sleep(3)
    print(f"  [{worker_name}] Runtime deleted")


# ---------------------------------------------------------------------------
# Output checking
# ---------------------------------------------------------------------------
def get_output_and_blocked_count(driver):
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
# Main
# ---------------------------------------------------------------------------
def main():
    # Prompt for credentials
    username = input("Google email: ").strip()
    password = getpass.getpass("Google password: ")

    # Launch Chrome if needed
    launch_chrome()

    print("[*] Connecting to Chrome …")
    driver = connect()
    print("[+] Connected")

    # --- Initial setup ---
    print(f"\n[*] Setting up {NUM_TABS} Colab tabs …")
    ensure_signed_in_first_tab(driver, username, password)
    time.sleep(3)

    if NUM_TABS > 1:
        open_additional_tabs(driver, NUM_TABS - 1)
        time.sleep(5)

    all_handles = driver.window_handles[:NUM_TABS]
    tab_workers = {}
    for i, handle in enumerate(all_handles):
        tab_workers[handle] = WORKER_NAMES[i]

    # --- Setup and run on each tab ---
    for handle, worker_name in tab_workers.items():
        driver.switch_to.window(handle)
        time.sleep(2)
        ok = setup_and_run(driver, worker_name)
        if not ok:
            print(f"  [{worker_name}] Setup failed — will retry later")

    # --- Monitor loop ---
    print(f"\n[*] All tabs running. Monitoring (interval={MONITOR_INTERVAL}s) …")
    restart_queue = {}  # handle -> restart_at timestamp

    while True:
        try:
            now = time.time()
            status_parts = []

            # Restart tabs that finished cooldown
            for handle in list(restart_queue.keys()):
                if now >= restart_queue[handle]:
                    worker_name = tab_workers[handle]
                    print(f"\n[*] Restarting {worker_name} …")
                    driver.switch_to.window(handle)
                    driver.close()
                    remaining = [h for h in driver.window_handles if h in tab_workers]
                    driver.switch_to.window(remaining[0])
                    new_handle = open_single_tab(driver, username, password)
                    time.sleep(3)

                    del tab_workers[handle]
                    del restart_queue[handle]
                    tab_workers[new_handle] = worker_name

                    driver.switch_to.window(new_handle)
                    time.sleep(2)
                    setup_and_run(driver, worker_name)

            # Monitor each active tab
            for handle, worker_name in list(tab_workers.items()):
                if handle in restart_queue:
                    status_parts.append(f"{worker_name}=COOLDOWN")
                    continue

                driver.switch_to.window(handle)
                tail, blocked = get_output_and_blocked_count(driver)
                status_parts.append(f"{worker_name}(b={blocked})")

                if blocked > BOT_BLOCKED_THRESHOLD:
                    print(f"\n[!] {worker_name}: bot_blocked={blocked} > {BOT_BLOCKED_THRESHOLD}!")
                    teardown_runtime(driver, worker_name)
                    restart_at = time.time() + COOLDOWN_SECONDS
                    restart_queue[handle] = restart_at
                    print(f"  [{worker_name}] Will restart at {time.strftime('%H:%M:%S', time.localtime(restart_at))}")

            print(f"\r[{time.strftime('%H:%M:%S')}] {' | '.join(status_parts)}", end="", flush=True)
            time.sleep(MONITOR_INTERVAL)

        except KeyboardInterrupt:
            print("\n[*] Stopped.")
            break
        except Exception as e:
            print(f"\n[!] Error: {e}")
            time.sleep(5)

    print("[*] Done.")


if __name__ == "__main__":
    main()
