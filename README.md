# YouTube Mining — Colab Runner

Automates running `ytminer-download` across multiple Google Colab notebooks, with multiple Google accounts in parallel.

## What it does

1. Reads N Google accounts from `accounts.txt`
2. Launches a separate Chrome window per account (with persistent profiles)
3. Opens 3 Colab tabs per window, writes and runs the ytminer-download command
4. Monitors output — when 2+ tabs in a window report `bot_blocked`, tears down ALL runtimes in that window and restarts
5. A supervisor process watches all workers, auto-restarts crashed ones, and prints a status dashboard

## Requirements

- **Python 3.8+**
- **Google Chrome** installed

## Setup

```bash
pip install -r requirements.txt
```

Add your accounts to `accounts.txt` (one username and password per pair, separated by blank lines):

```
username
mypassword

username
mypassword
```

## Usage

```bash
# Verify config without launching anything
python start_colab.py --dry-run

# Run all accounts
python start_colab.py
```

Press `Ctrl+C` to gracefully stop all workers and Chrome instances.

## Files

| File | Description |
|---|---|
| `start_colab.py` | Supervisor — parses accounts, spawns workers, monitors health |
| `worker.py` | Per-account worker — Chrome, Colab automation, monitoring |
| `accounts.txt` | Google account credentials (username/password pairs) |
| `profiles/` | Persistent Chrome profiles per account (auto-created) |
| `logs/` | Per-account log files |
| `start_colab_legacy.py` | Previous single-account version (kept for reference) |

## Config

Edit constants at the top of `worker.py`:

| Variable | Default | Description |
|---|---|---|
| `NUM_TABS` | 3 | Colab tabs per Chrome window |
| `MONITOR_INTERVAL` | 15 | Seconds between output checks |
| `BOT_BLOCKED_TAB_THRESHOLD` | 1 | bot_blocked count for a tab to be "blocked" |
| `BLOCKED_TABS_TO_RESTART` | 2 | How many blocked tabs trigger a full restart |
| `RESTART_DELAY` | 30 | Seconds to wait after teardown before re-running |

Edit constants at the top of `start_colab.py`:

| Variable | Default | Description |
|---|---|---|
| `BASE_DEBUG_PORT` | 9222 | First Chrome debug port (increments per account) |
| `STAGGER_DELAY` | 10 | Seconds between launching each worker |
| `MAX_CONSECUTIVE_RESTARTS` | 5 | Max auto-restarts per worker before giving up |

## Chrome Profiles

On first run, each account gets a fresh Chrome profile in `profiles/<username>/`. Sign-in is required once — cookies are saved for future runs. To reuse profiles from another machine, copy them into the `profiles/` directory.

## Troubleshooting

- **Chrome closes immediately**: Make sure no other Chrome is running with the same profile. Close all Chrome windows first.
- **Sign-in fails / 2FA**: The script detects 2FA and waits up to 5 minutes for manual completion. Watch the Chrome window and complete the challenge.
- **Port conflicts**: If a debug port is in use, close the process using it. Run `--dry-run` to check.
- **Worker keeps crashing**: Check `logs/<username>.log` for detailed error traces.
