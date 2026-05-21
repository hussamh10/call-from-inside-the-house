"""
manage_sessions.py
List or terminate active Colab runtime sessions for each account.

For each account in accounts.txt, this launches a fresh Chrome with that
account's persistent profile (profiles/<username>/), opens Colab,
opens "Runtime → Manage sessions", and either lists or terminates every
session it finds.

Workers MUST be stopped before running this — Chrome will refuse to
launch with a locked profile.  If a worker's Chrome is detected on its
debug port (BASE_DEBUG_PORT + i), the account is skipped.

Usage:
    python manage_sessions.py                  # terminate all, every account
    python manage_sessions.py --account NAME   # one account only
    python manage_sessions.py --list           # list sessions, don't terminate
    python manage_sessions.py --port-base 9422 # override standalone port base
"""

import argparse
import os
import socket
import subprocess
import sys
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import (
    JavascriptException,
    WebDriverException,
)

from worker import (
    CHROME_FLAGS,
    COLAB_URL,
    find_chrome_binary,
    setup_logger,
    js,
    click_runtime_menu_item,
    kill_chrome,
)
from start_colab import parse_accounts, ACCOUNTS_FILE, BASE_DEBUG_PORT

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILES_DIR = os.path.join(BASE_DIR, "profiles")

# Default port base — offset well above worker.py (9222+) and
# sign_in_profiles.py (9322+) to avoid collisions.
DEFAULT_PORT_BASE = 9422

# Seconds to wait after opening "Manage sessions" for the dialog to populate.
# Colab pulls the session list from the backend, which can take 30–60s.
SESSION_LOAD_WAIT = 45


# ---------------------------------------------------------------------------
# Chrome lifecycle
# ---------------------------------------------------------------------------
def port_reachable(port, timeout=1.5):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except (ConnectionRefusedError, OSError):
        return False


def wait_for_port(port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if port_reachable(port):
            return True
        time.sleep(1)
    return False


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
        "--remote-allow-origins=*",
    ] + CHROME_FLAGS
    logger.info("Launching Chrome (port=%d profile=%s)", port, profile_dir)
    try:
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Chrome binary missing at: %s", chrome_path)
        return None


def connect_to_chrome(port, logger):
    opts = Options()
    opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    driver = webdriver.Chrome(options=opts, service=ChromeService())
    logger.info("Selenium attached on port %d", port)
    return driver


# ---------------------------------------------------------------------------
# Manage-sessions dialog
# ---------------------------------------------------------------------------
# Helper: walk the entire DOM including shadow roots.  Colab nests
# everything inside custom elements with closed-style shadow trees, so
# plain querySelector is not enough.
DEEP_WALK_FN = r"""
function deepWalk(root, fn, depth) {
    depth = depth || 0;
    if (!root || depth > 60) return;
    try { fn(root); } catch(e) {}
    try {
        if (root.shadowRoot) {
            for (const c of root.shadowRoot.children) deepWalk(c, fn, depth + 1);
        }
    } catch(e) {}
    try {
        for (const c of (root.children || [])) deepWalk(c, fn, depth + 1);
    } catch(e) {}
}
"""


# Find open dialogs that look like the "Manage sessions" dialog.
FIND_DIALOGS_FN = r"""
function findSessionsDialogs() {
    const out = [];
    deepWalk(document.documentElement, (n) => {
        if (!n.tagName) return;
        const tag = n.tagName.toLowerCase();
        const isDialog = tag === 'mwc-dialog'
            || tag === 'colab-sessions-dialog'
            || (n.getAttribute && n.getAttribute('role') === 'dialog');
        if (!isDialog) return;
        const isOpen = (n.open === true)
            || (n.hasAttribute && n.hasAttribute('open'))
            || (n.offsetParent !== null);
        if (!isOpen) return;
        const text = (n.innerText || n.textContent || '').toLowerCase();
        // Heuristic: this is the manage-sessions dialog if it mentions sessions/runtime
        if (/active sessions|manage sessions|terminate/i.test(text)) {
            out.push(n);
        }
    });
    return out;
}
"""


LIST_SESSIONS_JS = DEEP_WALK_FN + FIND_DIALOGS_FN + r"""
const dialogs = findSessionsDialogs();
const out = [];
for (const d of dialogs) {
    deepWalk(d, (n) => {
        if (!n.tagName) return;
        const tag = n.tagName.toLowerCase();
        const isRow = tag === 'tr' || n.getAttribute?.('role') === 'row';
        if (!isRow) return;
        // Skip header rows (those that only contain <th>)
        const cells = (n.children || []);
        const hasTd = Array.from(cells).some(
            c => c.tagName && c.tagName.toLowerCase() === 'td');
        if (cells.length > 0 && !hasTd && tag === 'tr') return;
        const text = (n.innerText || n.textContent || '').trim();
        if (text.length < 3 || text.length > 600) return;
        out.push(text.replace(/\s+/g, ' ').slice(0, 200));
    });
}
return out;
"""


DUMP_DIALOG_JS = DEEP_WALK_FN + FIND_DIALOGS_FN + r"""
const dialogs = findSessionsDialogs();
const out = [];
for (const d of dialogs) {
    const tag = d.tagName.toLowerCase();
    const text = (d.innerText || d.textContent || '').trim().replace(/\s+/g, ' ');
    out.push(`<${tag}>: ` + text.slice(0, 800));
}
if (out.length === 0) return '(no sessions-dialog found — open dialogs only:)\n'
    + Array.from(document.querySelectorAll('mwc-dialog[open],[role=dialog]'))
        .map(d => '<' + d.tagName.toLowerCase() + '> '
                + ((d.innerText||d.textContent||'').trim().replace(/\s+/g,' ').slice(0,400)))
        .join('\n');
return out.join('\n\n');
"""


# Click any element inside the Manage Sessions dialog that looks like a
# "terminate" affordance — text buttons, icon buttons, or close-style icons.
CLICK_TERMINATE_JS = DEEP_WALK_FN + FIND_DIALOGS_FN + r"""
const dialogs = findSessionsDialogs();
if (dialogs.length === 0) return -1;  // signal: no dialog at all

const targets = [];
const TERMINATE_TEXTS = new Set([
    'terminate', 'terminate other sessions', 'terminate all',
    'disconnect', 'delete'
]);
const TERMINATE_ARIA_RE = /terminate|delete runtime|disconnect/i;

for (const d of dialogs) {
    deepWalk(d, (n) => {
        if (!n.tagName) return;
        const tag = n.tagName.toLowerCase();

        // Text-based buttons
        if (['button','mwc-button','md-text-button','paper-button','a'].includes(tag)) {
            const text = (n.innerText || n.textContent || '').trim().toLowerCase();
            if (TERMINATE_TEXTS.has(text)) { targets.push(n); return; }
        }

        // Icon-only buttons — use aria-label / title / data-tooltip
        if (['mwc-icon-button','colab-close-button','button'].includes(tag)) {
            const aria = (n.getAttribute('aria-label') || '').toLowerCase();
            const title = (n.getAttribute('title') || '').toLowerCase();
            const tooltip = (n.getAttribute('data-tooltip') || '').toLowerCase();
            const hint = aria + ' ' + title + ' ' + tooltip;
            if (TERMINATE_ARIA_RE.test(hint)) targets.push(n);
        }

        // Generic close icons (e.g., <iron-icon icon="close"> wrapped in clickable parents)
        if (tag === 'mwc-icon-button') {
            const icon = (n.getAttribute('icon') || '').toLowerCase();
            if (icon === 'close' || icon === 'delete') {
                // Only count close icons that sit inside a table row
                let p = n.parentElement;
                while (p) {
                    const ptag = p.tagName?.toLowerCase();
                    if (ptag === 'tr' || p.getAttribute?.('role') === 'row') {
                        targets.push(n); break;
                    }
                    p = p.parentElement;
                }
            }
        }
    });
}

// De-duplicate
const unique = [];
const seen = new WeakSet();
for (const t of targets) {
    if (!seen.has(t)) { seen.add(t); unique.push(t); }
}

let clicked = 0;
for (const t of unique) {
    try { t.scrollIntoView({block: 'center'}); t.click(); clicked++; } catch(e) {}
}
return clicked;
"""


CONFIRM_DIALOGS_JS = DEEP_WALK_FN + r"""
const targets = [];
document.querySelectorAll('mwc-dialog[open]').forEach((d) => {
    deepWalk(d, (n) => {
        if (!n.tagName) return;
        const tag = n.tagName.toLowerCase();
        if (!['button','mwc-button','md-text-button','paper-button'].includes(tag)) return;
        const text = (n.innerText || n.textContent || '').trim().toLowerCase();
        if (['yes','terminate','ok','confirm'].includes(text)) targets.push(n);
    });
});
let clicked = 0;
for (const t of targets) {
    try { t.click(); clicked++; } catch(e) {}
}
return clicked;
"""


def open_manage_sessions(driver, logger, load_wait=SESSION_LOAD_WAIT):
    """Navigate to Colab, open Runtime → Manage sessions, wait for it to populate."""
    driver.get(COLAB_URL)
    time.sleep(6)

    if "accounts.google.com" in driver.current_url:
        logger.error("Profile not signed in — run sign_in_profiles.py first")
        return False

    if not click_runtime_menu_item(driver, "Manage sessions", logger):
        logger.error("Could not open 'Manage sessions' menu item")
        return False

    logger.info("Waiting up to %ds for sessions to load ...", load_wait)
    deadline = time.time() + load_wait
    last_count = -1
    while time.time() < deadline:
        try:
            n = len(js(driver, LIST_SESSIONS_JS) or [])
        except (JavascriptException, WebDriverException):
            n = 0
        if n != last_count:
            logger.info("  loaded so far: %d session(s)", n)
            last_count = n
        time.sleep(5)
    logger.info("Session load wait complete (%d session(s) visible)", last_count)
    return True


def list_sessions(driver, logger):
    try:
        sessions = js(driver, LIST_SESSIONS_JS) or []
    except (JavascriptException, WebDriverException) as exc:
        logger.error("List JS failed: %s", exc)
        return []
    for i, s in enumerate(sessions, 1):
        logger.info("  Session %d: %s", i, s)
    return sessions


def dump_dialog(driver, logger):
    """Log raw text of any visible Manage Sessions dialog (for diagnosis)."""
    try:
        text = js(driver, DUMP_DIALOG_JS) or "(no dialog)"
    except (JavascriptException, WebDriverException) as exc:
        logger.error("Dump JS failed: %s", exc)
        return
    logger.info("Dialog dump: %s", text[:1200])


def terminate_all_sessions(driver, logger):
    """Click every Terminate button, confirm dialogs, repeat until none left."""
    total = 0
    for round_num in range(1, 21):
        try:
            n = js(driver, CLICK_TERMINATE_JS)
        except (JavascriptException, WebDriverException) as exc:
            logger.error("Terminate JS failed: %s", exc)
            break
        if n is None:
            n = 0
        if n == -1:
            logger.warning("Round %d: sessions dialog not found", round_num)
            break
        if n == 0:
            logger.info("Round %d: no more terminate buttons", round_num)
            break
        total += n
        logger.info("Round %d: clicked %d terminate button(s)", round_num, n)
        time.sleep(2)

        # Confirm any dialogs that opened
        for _ in range(8):
            try:
                confirmed = js(driver, CONFIRM_DIALOGS_JS) or 0
            except (JavascriptException, WebDriverException):
                confirmed = 0
            if not confirmed:
                break
            logger.info("Confirmed %d dialog button(s)", confirmed)
            time.sleep(1)
        time.sleep(2)
    return total


# ---------------------------------------------------------------------------
# Per-account driver
# ---------------------------------------------------------------------------
def process_account(username, account_index, port, profile_dir,
                    list_only, load_wait, logger):
    # Refuse to run if worker's Chrome is using this profile.
    worker_port = BASE_DEBUG_PORT + account_index
    if port_reachable(worker_port):
        logger.error(
            "Worker Chrome appears to be running on port %d — stop workers "
            "(Ctrl+C the start_colab.py session) before running this script.",
            worker_port,
        )
        return False

    if port_reachable(port):
        logger.error("Port %d is already in use; pick a different --port-base",
                     port)
        return False

    chrome_proc = None
    driver = None
    try:
        chrome_proc = launch_chrome(port, profile_dir, logger)
        if not chrome_proc:
            return False
        if not wait_for_port(port, timeout=20):
            logger.error("Chrome did not come up on port %d", port)
            return False

        driver = connect_to_chrome(port, logger)
        if not open_manage_sessions(driver, logger, load_wait=load_wait):
            return False

        sessions = list_sessions(driver, logger)
        logger.info("Found %d session(s) by row-scan", len(sessions))
        if not sessions:
            # Listing came up empty — dump the dialog so we know what's there.
            dump_dialog(driver, logger)
        if list_only:
            return True

        # Always attempt termination — the click pass uses different (broader)
        # selectors than the row listing, so it can succeed even when the
        # row count was 0.
        clicked = terminate_all_sessions(driver, logger)
        logger.info("Total terminate clicks: %d", clicked)
        time.sleep(3)

        # Re-list to confirm
        remaining = list_sessions(driver, logger)
        logger.info("Remaining session(s): %d", len(remaining))
        return True

    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
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
    ap = argparse.ArgumentParser(
        description="List or terminate Colab runtime sessions per account",
    )
    ap.add_argument("--account", help="only process this username")
    ap.add_argument("--list", action="store_true",
                    help="list sessions only, do not terminate")
    ap.add_argument("--port-base", type=int, default=DEFAULT_PORT_BASE,
                    help=f"first debug port (default {DEFAULT_PORT_BASE})")
    ap.add_argument("--load-wait", type=int, default=SESSION_LOAD_WAIT,
                    help=f"seconds to wait for Manage sessions to populate "
                         f"(default {SESSION_LOAD_WAIT})")
    args = ap.parse_args()

    accounts = parse_accounts(ACCOUNTS_FILE)
    # Keep original indices for worker-port lookup, even when filtering.
    indexed = list(enumerate(accounts))
    if args.account:
        indexed = [(i, (u, p)) for i, (u, p) in indexed if u == args.account]
        if not indexed:
            print(f"[!] No account named {args.account!r}")
            sys.exit(1)

    action = "list" if args.list else "terminate"
    print(f"[+] {len(indexed)} account(s) to process ({action})")

    results = []
    for i, (username, _) in indexed:
        port = args.port_base + i
        profile_dir = os.path.join(PROFILES_DIR, username)
        logger = setup_logger(username)
        logger.info("=" * 60)
        logger.info("%s  port=%d  action=%s", username, port, action)
        ok = process_account(username, i, port, profile_dir,
                             args.list, args.load_wait, logger)
        results.append((username, ok))

    print("\n" + "=" * 60)
    print(f" {action.upper()} RESULTS")
    print("=" * 60)
    width = max(len(u) for u, _ in results) + 2
    for u, ok in results:
        print(f"  {u:<{width}}  {'OK' if ok else 'FAILED'}")
    print("=" * 60)
    if any(not ok for _, ok in results):
        sys.exit(2)


if __name__ == "__main__":
    main()
