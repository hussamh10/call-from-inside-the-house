"""
start_colab.py
Supervisor — reads accounts.txt, spawns one worker process per account,
monitors their health, restarts crashed workers, and prints a status dashboard.

Usage:
    python start_colab.py               # run all accounts
    python start_colab.py --dry-run     # parse + validate, don't launch
"""

import multiprocessing
import os
import signal
import socket
import sys
import time

from worker import run_worker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DEBUG_PORT = 9222
STAGGER_DELAY = 10          # seconds between launching workers
SUPERVISOR_POLL = 30        # seconds between health checks
DASHBOARD_INTERVAL = 60     # seconds between full dashboard prints
MAX_CONSECUTIVE_RESTARTS = 5
RESTART_WINDOW = 600        # seconds — reset restart counter after this

ACCOUNTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts.txt")


# ---------------------------------------------------------------------------
# Account parsing
# ---------------------------------------------------------------------------
def parse_accounts(filepath):
    """Parse accounts.txt.  Returns list of (username, password).

    Format: alternating username / password lines separated by blank lines.
    Skips the first entry if username is literally 'account' (placeholder).
    """
    if not os.path.isfile(filepath):
        print(f"[!] accounts.txt not found at {filepath}")
        sys.exit(1)

    with open(filepath) as f:
        lines = [l.strip() for l in f.readlines()]

    accounts = []
    i = 0
    while i < len(lines):
        # Skip blank lines
        while i < len(lines) and lines[i] == "":
            i += 1
        if i >= len(lines):
            break
        username = lines[i]
        i += 1
        # Skip blank lines between username and password
        while i < len(lines) and lines[i] == "":
            i += 1
        if i >= len(lines):
            print(f"[!] Account '{username}' has no password")
            sys.exit(1)
        password = lines[i]
        i += 1
        accounts.append((username, password))

    # Skip placeholder
    if accounts and accounts[0][0].lower() == "account":
        accounts = accounts[1:]

    if not accounts:
        print("[!] No accounts found in accounts.txt")
        sys.exit(1)

    return accounts


# ---------------------------------------------------------------------------
# Port validation
# ---------------------------------------------------------------------------
def check_port_free(port):
    """Return True if the port is not in use."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect(("127.0.0.1", port))
        s.close()
        return False  # port is in use
    except (ConnectionRefusedError, OSError):
        return True


def validate_ports(accounts):
    """Check that all needed debug ports are free."""
    issues = []
    for i, (username, _) in enumerate(accounts):
        port = BASE_DEBUG_PORT + i
        if not check_port_free(port):
            issues.append(f"  Port {port} (for {username}) is already in use")
    return issues


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
def print_dashboard(accounts, status_dict, restart_counts, processes):
    ts = time.strftime("%H:%M:%S")
    width = max(len(u) for u, _ in accounts) + 2
    lines = [f"\n{'='*60}", f" Status [{ts}]", f"{'='*60}"]
    for i, (username, _) in enumerate(accounts):
        port = BASE_DEBUG_PORT + i
        status = status_dict.get(username, "UNKNOWN")
        restarts = restart_counts.get(username, 0)
        alive = "alive" if username in processes and processes[username].is_alive() else "DEAD"
        lines.append(
            f" {username:<{width}} | port {port} | {alive:<5} | restarts: {restarts} | {status}"
        )
    lines.append(f"{'='*60}")
    print("\n".join(lines), flush=True)


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------
def main():
    dry_run = "--dry-run" in sys.argv

    accounts = parse_accounts(ACCOUNTS_FILE)
    print(f"[+] Loaded {len(accounts)} account(s):")
    for i, (username, _) in enumerate(accounts):
        port = BASE_DEBUG_PORT + i
        base_dir = os.path.dirname(os.path.abspath(__file__))
        profile = os.path.join(base_dir, "profiles", username)
        print(f"    {i+1}. {username}  port={port}  profile={profile}")

    # Validate ports
    port_issues = validate_ports(accounts)
    if port_issues:
        print("\n[!] Port conflicts:")
        for issue in port_issues:
            print(issue)
        if not dry_run:
            print("[!] Close the processes using these ports, then try again.")
            sys.exit(1)

    if dry_run:
        print("\n[*] Dry run complete — everything looks good.")
        return

    # Shared state
    manager = multiprocessing.Manager()
    shutdown_event = manager.Event()
    status_dict = manager.dict()

    # Track restarts
    restart_counts = {}       # username -> count
    last_restart_time = {}    # username -> timestamp

    processes = {}  # username -> Process

    # Signal handler
    def handle_signal(signum, frame):
        print("\n[*] Shutdown signal received — stopping all workers ...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Spawn workers with staggered starts
    print(f"\n[*] Spawning {len(accounts)} workers ...")
    for i, (username, password) in enumerate(accounts):
        if shutdown_event.is_set():
            break
        p = multiprocessing.Process(
            target=run_worker,
            args=(i, username, password, shutdown_event, status_dict),
            name=f"worker-{username}",
            daemon=True,
        )
        p.start()
        processes[username] = p
        restart_counts[username] = 0
        last_restart_time[username] = 0
        print(f"[+] Started worker for {username} (pid={p.pid})")
        if i < len(accounts) - 1:
            time.sleep(STAGGER_DELAY)

    # Supervisor loop
    print(f"\n[*] All workers launched. Monitoring (poll={SUPERVISOR_POLL}s) ...")
    last_dashboard = 0

    while not shutdown_event.is_set():
        try:
            time.sleep(SUPERVISOR_POLL)
        except KeyboardInterrupt:
            shutdown_event.set()
            break

        now = time.time()

        # Check worker health
        for i, (username, password) in enumerate(accounts):
            if username not in processes:
                continue
            p = processes[username]
            if not p.is_alive():
                exit_code = p.exitcode
                print(f"\n[!] Worker {username} died (exit={exit_code})")

                # Reset counter if outside restart window
                if now - last_restart_time.get(username, 0) > RESTART_WINDOW:
                    restart_counts[username] = 0

                if restart_counts.get(username, 0) >= MAX_CONSECUTIVE_RESTARTS:
                    print(f"[!] {username}: too many restarts ({MAX_CONSECUTIVE_RESTARTS}) — giving up")
                    status_dict[username] = "FAILED_TOO_MANY_RESTARTS"
                    del processes[username]
                    continue

                restart_counts[username] = restart_counts.get(username, 0) + 1
                last_restart_time[username] = now
                print(f"[*] Restarting {username} (attempt {restart_counts[username]}) ...")

                p = multiprocessing.Process(
                    target=run_worker,
                    args=(i, username, password, shutdown_event, status_dict),
                    name=f"worker-{username}",
                    daemon=True,
                )
                p.start()
                processes[username] = p
                print(f"[+] Restarted {username} (pid={p.pid})")

        # Dashboard
        if now - last_dashboard >= DASHBOARD_INTERVAL:
            print_dashboard(accounts, status_dict, restart_counts, processes)
            last_dashboard = now

    # Graceful shutdown
    print("\n[*] Waiting for workers to stop ...")
    deadline = time.time() + 30
    for username, p in processes.items():
        remaining = max(0, deadline - time.time())
        p.join(timeout=remaining)
        if p.is_alive():
            print(f"[!] Force-terminating {username}")
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()

    print("[*] All workers stopped. Done.")


if __name__ == "__main__":
    main()
