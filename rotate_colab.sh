#!/usr/bin/env bash
# rotate_colab.sh
# Run a rotating cycle:
#   1. Terminate any existing Colab sessions (manage_sessions.py)
#   2. Start workers (start_colab.py) in the background
#   3. Wait CYCLE_HOURS hours
#   4. Stop workers (SIGINT, then SIGKILL if needed)
#   5. Loop back to step 1
#
# If start_colab.py exits on its own before the cycle window is up, the
# rotator immediately starts a fresh cycle (terminate + relaunch).
#
# Usage:
#   ./rotate_colab.sh                  # default: 3-hour cycles
#   CYCLE_HOURS=2 ./rotate_colab.sh    # 2-hour cycles
#   PYTHON=python3 ./rotate_colab.sh   # custom interpreter
#
# Press Ctrl+C to stop cleanly.

set -u

CYCLE_HOURS="${CYCLE_HOURS:-3}"
PYTHON="${PYTHON:-python}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-90}"   # seconds to wait for start_colab.py to stop
CHROME_SETTLE="${CHROME_SETTLE:-10}"         # seconds after stop, before next cycle

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

for f in manage_sessions.py start_colab.py; do
    [ -f "$f" ] || { echo "[rotate] missing $f in $SCRIPT_DIR"; exit 1; }
done

WORKER_PID=""

log() {
    printf '[rotate %s] %s\n' "$(date +%H:%M:%S)" "$*"
}

# Force-kill any Chrome that's still holding one of our profile dirs.
# Scoped by user-data-dir so we don't touch the user's normal Chrome.
kill_leftover_chrome() {
    if command -v pkill >/dev/null 2>&1; then
        pkill -f "user-data-dir=${SCRIPT_DIR}/profiles/" 2>/dev/null || true
    fi
}

stop_workers() {
    if [ -z "$WORKER_PID" ]; then
        return 0
    fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        WORKER_PID=""
        return 0
    fi

    log "Sending SIGINT to start_colab.py (pid=$WORKER_PID); waiting up to ${SHUTDOWN_TIMEOUT}s ..."
    kill -INT "$WORKER_PID" 2>/dev/null || true
    local i
    for i in $(seq 1 "$SHUTDOWN_TIMEOUT"); do
        if ! kill -0 "$WORKER_PID" 2>/dev/null; then
            log "start_colab.py exited cleanly"
            WORKER_PID=""
            kill_leftover_chrome
            return 0
        fi
        sleep 1
    done

    log "Graceful shutdown timed out; force-killing pid=$WORKER_PID"
    kill -TERM "$WORKER_PID" 2>/dev/null || true
    sleep 3
    kill -KILL "$WORKER_PID" 2>/dev/null || true
    WORKER_PID=""
    kill_leftover_chrome
}

cleanup_and_exit() {
    log "Signal received; shutting down ..."
    stop_workers
    log "Stopped."
    exit 0
}

trap cleanup_and_exit INT TERM

log "Rotator starting — cycle = ${CYCLE_HOURS}h, shutdown timeout = ${SHUTDOWN_TIMEOUT}s"

while true; do
    log "=== Cycle start ==="

    log "[1/3] Terminating any existing Colab sessions ..."
    "$PYTHON" manage_sessions.py || log "manage_sessions.py exit=$? (continuing)"

    log "[2/3] Starting start_colab.py ..."
    "$PYTHON" start_colab.py &
    WORKER_PID=$!
    log "start_colab.py running (pid=$WORKER_PID)"

    cycle_seconds=$((CYCLE_HOURS * 3600))
    end_time=$(($(date +%s) + cycle_seconds))
    end_pretty=$(date -r "$end_time" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "+${CYCLE_HOURS}h")
    log "[3/3] Running for ${CYCLE_HOURS}h (until $end_pretty)"

    while [ "$(date +%s)" -lt "$end_time" ]; do
        if ! kill -0 "$WORKER_PID" 2>/dev/null; then
            log "start_colab.py exited on its own; restarting cycle immediately"
            WORKER_PID=""
            break
        fi
        sleep 30
    done

    log "Cycle window over; rotating ..."
    stop_workers
    log "Waiting ${CHROME_SETTLE}s for Chrome processes to fully exit ..."
    sleep "$CHROME_SETTLE"
done
