"""
sign_in_profiles.py
One-time setup: for each account in accounts.txt, launch Chrome with that
account's profile dir and sign Chrome *itself* in (Turn on sync) so the
profile is permanently associated with the Google account.

After this finishes, future runs of worker.py see signed-in profiles.

Usage:
    python sign_in_profiles.py                     # all accounts
    python sign_in_profiles.py --account NAME      # one account
    python sign_in_profiles.py --force             # redo even if already signed in
    python sign_in_profiles.py --port-base 9322    # avoid clashing with worker.py
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    JavascriptException,
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)

from worker import (
    CHROME_FLAGS,
    find_chrome_binary,
    setup_logger,
    js,
    wait_js,
)
from start_colab import parse_accounts, ACCOUNTS_FILE, BASE_DEBUG_PORT

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CHROME_SYNC_SIGNIN_URL = "https://accounts.google.com/signin/chrome/sync?ssp=1"
MANUAL_2FA_TIMEOUT = 600   # seconds — manual 2FA window
CONFIRM_POLL_TIMEOUT = 90  # seconds — wait for sync-confirmation page

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILES_DIR = os.path.join(BASE_DIR, "profiles")

# Anti-detection flags on top of CHROME_FLAGS — Google web sign-in
# is increasingly hostile to obvious automation.
EXTRA_SIGNIN_FLAGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-default-browser-check",
]


# ---------------------------------------------------------------------------
# Chrome lifecycle (mirrors worker.py but with extra flags + remote-allow-origins)
# ---------------------------------------------------------------------------
def launch_chrome(port, profile_dir, logger):
    chrome_path = find_chrome_binary()
    if not chrome_path:
        logger.error("Chrome not found")
        return None
    os.makedirs(profile_dir, exist_ok=True)
    cmd = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        # required so Selenium can attach in Chrome >=111
        "--remote-allow-origins=*",
    ] + CHROME_FLAGS + EXTRA_SIGNIN_FLAGS
    logger.info("Launching Chrome (port=%d profile=%s)", port, profile_dir)
    try:
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Chrome binary missing at: %s", chrome_path)
        return None


def wait_for_chrome(port, timeout=20.0, logger=None):
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
        logger.error("Chrome debug port %d unreachable after %.0fs", port, timeout)
    return False


def connect_to_chrome(port, logger):
    opts = Options()
    opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    driver = webdriver.Chrome(options=opts, service=ChromeService())
    logger.info("Selenium attached on port %d", port)
    return driver


def kill_chrome(proc, logger, timeout=10):
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
        logger.info("Chrome closed cleanly")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        logger.warning("Chrome force-killed")
    except Exception as exc:
        logger.warning("kill_chrome: %s", exc)


# ---------------------------------------------------------------------------
# Profile state introspection
# ---------------------------------------------------------------------------
def read_profile_state(profile_dir):
    """Parse Local State and return signed-in info for the Default profile."""
    ls_path = os.path.join(profile_dir, "Local State")
    if not os.path.isfile(ls_path):
        return {}
    try:
        with open(ls_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    info = data.get("profile", {}).get("info_cache", {}).get("Default", {})
    return {
        "gaia_id":     info.get("gaia_id") or "",
        "user_name":   info.get("user_name") or "",
        "gaia_name":   info.get("gaia_name") or "",
        "is_consented": bool(info.get("is_consented_primary_account", False)),
        "profile_name": info.get("name") or "",
    }


def is_signed_in(profile_dir):
    s = read_profile_state(profile_dir)
    return bool(s.get("gaia_id"))


# ---------------------------------------------------------------------------
# Sign-in flow
# ---------------------------------------------------------------------------
def _enter_email(driver, username, logger):
    logger.info("Entering email")
    try:
        el = WebDriverWait(driver, 30).until(
            lambda d: d.find_element(By.CSS_SELECTOR, "input[type='email']")
        )
    except TimeoutException:
        logger.error("Email field never appeared (url=%s)", driver.current_url)
        return False
    el.clear()
    email = username if "@" in username else f"{username}@gmail.com"
    el.send_keys(email)
    js(driver, """
        (document.querySelector('#identifierNext button') ||
         document.querySelector('#identifierNext')).click();
    """)
    time.sleep(4)
    return True


def _enter_password(driver, password, logger):
    logger.info("Entering password")
    try:
        el = WebDriverWait(driver, 45).until(
            lambda d: d.find_element(By.CSS_SELECTOR, "input[type='password']")
        )
    except TimeoutException:
        logger.error("Password field never appeared (url=%s)", driver.current_url)
        body = (js(driver, "return document.body.innerText || '';") or "")[:400]
        logger.debug("Page text head: %s", body.replace("\n", " | "))
        return False
    el.clear()
    el.send_keys(password)
    js(driver, """
        (document.querySelector('#passwordNext button') ||
         document.querySelector('#passwordNext')).click();
    """)
    time.sleep(4)
    return True


def _wait_for_credentials_complete(driver, logger):
    """Wait for credential entry to finish — landing on chrome://, sync intercept,
    or any non-accounts.google.com URL. Handle manual 2FA window."""
    deadline = time.time() + MANUAL_2FA_TIMEOUT
    warned_2fa = False
    last_logged_url = None
    while time.time() < deadline:
        try:
            url = driver.current_url
        except WebDriverException as exc:
            logger.error("Driver lost: %s", exc)
            return False

        if url != last_logged_url:
            logger.info("URL: %s", url)
            last_logged_url = url

        if (url.startswith("chrome://")
                or "sync-confirmation" in url
                or "signin/intercept" in url):
            logger.info("Reached Chrome confirmation/intercept (url=%s)", url)
            return True
        if "accounts.google.com" not in url and "google.com" in url:
            logger.info("Off accounts.google.com (url=%s)", url)
            return True
        if "google.com" not in url and url not in ("about:blank", "data:,"):
            logger.info("Off Google entirely (url=%s)", url)
            return True

        # 2FA / challenge detection
        if "accounts.google.com" in url:
            try:
                page_text = (js(driver, "return document.body.innerText || '';") or "").lower()
            except (JavascriptException, WebDriverException):
                page_text = ""
            challenge_keywords = [
                "2-step verification", "verify it's you", "verify it’s you",
                "confirm your identity", "security check", "enter the code",
                "verification code", "couldn’t sign you in",
                "this browser or app may not be secure",
            ]
            blocked = "browser or app may not be secure" in page_text or "couldn’t sign you in" in page_text
            if blocked:
                logger.error("Google blocked the sign-in as automated. "
                             "Try signing in manually once in this Chrome window, then re-run.")
                # leave the window open for the manual recovery window
            if not warned_2fa and any(k in page_text for k in challenge_keywords):
                logger.warning("Challenge detected — complete it manually in the Chrome window. "
                               "Waiting up to %d minutes …", MANUAL_2FA_TIMEOUT // 60)
                warned_2fa = True

        # Auto-dismiss "Not now" / "Skip" buttons on info screens
        try:
            js(driver, """
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const t = (b.innerText || '').trim().toLowerCase();
                    if (t === 'not now' || t === 'skip') { b.click(); break; }
                }
            """)
        except (JavascriptException, WebDriverException):
            pass
        time.sleep(3)

    logger.error("Credential phase timed out")
    return False


CONFIRM_SYNC_JS = r"""
function deepQuery(root, predicate, depth = 0) {
    if (!root || depth > 25) return null;
    try { if (predicate(root)) return root; } catch(e) {}
    let children = [];
    try {
        if (root.shadowRoot) children.push(...root.shadowRoot.querySelectorAll('*'));
    } catch(e) {}
    try {
        children.push(...(root.children || []));
    } catch(e) {}
    for (const c of children) {
        const r = deepQuery(c, predicate, depth + 1);
        if (r) return r;
    }
    return null;
}
const matchTexts = [
    "yes, i'm in", "yes, i’m in", "yes i'm in",
    "turn on sync", "turn on", "agree and continue", "i agree",
    "confirm", "accept", "ok, got it",
];
const buttonTags = new Set(['cr-button','button','md-text-button','mwc-button','paper-button']);
const el = deepQuery(document.documentElement, (n) => {
    if (!n || !n.tagName) return false;
    if (!buttonTags.has(n.tagName.toLowerCase())) return false;
    const t = (n.innerText || n.textContent || '').trim().toLowerCase();
    return matchTexts.some(m => t.includes(m));
});
if (el) {
    try { el.scrollIntoView(); } catch(e) {}
    el.click();
    return el.tagName + ':' + (el.innerText || el.textContent || '').trim().slice(0, 60);
}
return null;
"""


def _confirm_chrome_sync(driver, logger):
    """Click the 'Yes, I'm in' / Turn-on-sync button on the
    chrome://sync-confirmation or DICE intercept page."""
    deadline = time.time() + CONFIRM_POLL_TIMEOUT
    seen_urls = set()
    while time.time() < deadline:
        try:
            url = driver.current_url
        except WebDriverException as exc:
            logger.error("Driver lost during confirm: %s", exc)
            return False
        if url not in seen_urls:
            logger.info("Confirm-sync URL: %s", url)
            seen_urls.add(url)

        try:
            clicked = js(driver, CONFIRM_SYNC_JS)
        except (JavascriptException, WebDriverException) as exc:
            logger.debug("Confirm-sync JS error: %s", exc)
            clicked = None
        if clicked:
            logger.info("Clicked sync confirmation: %s", clicked)
            return True

        # If we've left the accounts/sync flow entirely, assume no confirm needed
        if ("accounts.google.com" not in url
                and "sync-confirmation" not in url
                and "signin/intercept" not in url
                and not url.startswith("chrome://signin")):
            logger.info("Past sign-in flow without explicit confirm (url=%s)", url)
            return True
        time.sleep(2)

    logger.warning("Sync-confirmation button never found within %ds", CONFIRM_POLL_TIMEOUT)
    return False


def sign_in_chrome_for_account(username, password, port, profile_dir, logger):
    chrome_proc = None
    driver = None
    try:
        chrome_proc = launch_chrome(port, profile_dir, logger)
        if not chrome_proc or not wait_for_chrome(port, logger=logger):
            return False
        driver = connect_to_chrome(port, logger)

        logger.info("Opening Chrome sync sign-in URL")
        driver.get(CHROME_SYNC_SIGNIN_URL)
        time.sleep(5)
        logger.info("Landed on %s", driver.current_url)

        if not _enter_email(driver, username, logger):
            return False
        if not _enter_password(driver, password, logger):
            return False
        if not _wait_for_credentials_complete(driver, logger):
            return False

        confirmed = _confirm_chrome_sync(driver, logger)
        # Let Chrome persist the new identity to Local State
        time.sleep(6)
        return confirmed

    except Exception as exc:
        logger.error("Unexpected error during sign-in: %s", exc, exc_info=True)
        return False
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        kill_chrome(chrome_proc, logger)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", help="only sign in this username")
    ap.add_argument("--force", action="store_true",
                    help="redo even if profile is already signed in")
    ap.add_argument("--port-base", type=int, default=BASE_DEBUG_PORT + 100,
                    help="first debug port (offsets +100 from worker default to avoid clashes)")
    args = ap.parse_args()

    accounts = parse_accounts(ACCOUNTS_FILE)
    if args.account:
        accounts = [(u, p) for u, p in accounts if u == args.account]
        if not accounts:
            print(f"[!] No account named {args.account!r} in accounts.txt")
            sys.exit(1)

    print(f"[+] {len(accounts)} account(s) to process")

    results = []  # (username, status, after_state)
    for i, (username, password) in enumerate(accounts):
        profile_dir = os.path.join(PROFILES_DIR, username)
        before = read_profile_state(profile_dir)
        already = bool(before.get("gaia_id"))

        if already and not args.force:
            print(f"\n[{username}] already signed in as {before['user_name']!r} "
                  f"(gaia_id={before['gaia_id'][:12]}…) — skipping (use --force to redo)")
            results.append((username, "skipped-already-signed-in", before))
            continue

        port = args.port_base + i
        logger = setup_logger(username)
        logger.info("=" * 60)
        logger.info("Signing in profile for %s (port=%d)", username, port)
        logger.info("Before: %s", before or "{no profile yet}")

        ok = sign_in_chrome_for_account(username, password, port, profile_dir, logger)
        time.sleep(2)  # let filesystem settle
        after = read_profile_state(profile_dir)
        verified = bool(after.get("gaia_id"))

        status = "OK" if verified else ("attempted-but-unverified" if ok else "FAILED")
        results.append((username, status, after))
        logger.info("After:  %s", after)
        logger.info("Result for %s: %s", username, status)

    # ---- summary ----
    print("\n" + "=" * 72)
    print(" SIGN-IN SUMMARY")
    print("=" * 72)
    width = max((len(u) for u, _, _ in results), default=10) + 2
    for username, status, state in results:
        gid = (state.get("gaia_id") or "")[:14]
        uname = state.get("user_name") or "-"
        print(f"  {username:<{width}} | {status:<28} | {uname} | gaia_id={gid}")
    print("=" * 72)

    failed = [u for u, s, _ in results if s not in ("OK", "skipped-already-signed-in")]
    if failed:
        print(f"[!] {len(failed)} account(s) need attention: {', '.join(failed)}")
        sys.exit(2)
    print("[+] All accounts signed in.")


if __name__ == "__main__":
    main()
