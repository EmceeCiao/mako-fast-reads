#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BIN="$REPO_ROOT/build/fastPathReadOnly"
if [ ! -x "$BIN" ]; then
    echo "Binary $BIN not found. Please run the build before invoking this benchmark." >&2
    exit 1
fi

USERNAME=${USER:-unknown}
LOG_DIR="$REPO_ROOT/fastpath-benchmark-logs"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

ACTIVE_PIDS=()

cleanup_files() {
    pkill -9 -f fastPathReadOnly 2>/dev/null || true
    rm -rf /tmp/${USERNAME}_mako_rocksdb_shard* 2>/dev/null || true
}

cleanup() {
    for pid in "${ACTIVE_PIDS[@]:-}"; do
        if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    ACTIVE_PIDS=()
    cleanup_files
}
trap cleanup EXIT

start_instance() {
    local mode="$1"
    local role="$2"
    local logfile="${LOG_DIR}/fastpath-${mode}-${role}.log"
    echo "Starting ${role} role for ${mode} mode (log: ${logfile})..."
    FAST_RO_MODE="$mode" "$BIN" 1 0 4 "$role" 1 >"$logfile" 2>&1 &
    local pid=$!
    ACTIVE_PIDS+=("$pid")
    echo "$pid"
}

run_mode() {
    local mode="$1"
    echo "========================================="
    echo "Running fast-path throughput benchmark (${mode})"
    echo "========================================="

    cleanup_files
    rm -f "${LOG_DIR}/fastpath-${mode}-"*.log 2>/dev/null || true

    PID_LOCALHOST=$(start_instance "$mode" "localhost")
    PID_LEARNER=$(start_instance "$mode" "learner")
    PID_P2=$(start_instance "$mode" "p2")
    sleep 1
    PID_P1=$(start_instance "$mode" "p1")

    echo "Waiting for leader process to finish (${mode})..."
    set +e
    wait "$PID_LOCALHOST"
    local leader_status=$?
    set -e
    if [ "$leader_status" -ne 0 ]; then
        echo "Leader process exited with status $leader_status in ${mode} mode." >&2
        return "$leader_status"
    fi

    for pid in "$PID_P1" "$PID_P2" "$PID_LEARNER"; do
        if [ -n "${pid:-}" ]; then
            set +e
            wait "$pid"
            set -e
        fi
    done

    ACTIVE_PIDS=()

    local leader_log="${LOG_DIR}/fastpath-${mode}-localhost.log"
    if [ ! -f "$leader_log" ]; then
        echo "Leader log $leader_log not found for mode $mode" >&2
        return 1
    fi

    if ! grep -q "FAST_PATH_READONLY_DONE" "$leader_log"; then
        echo "FAST_PATH_READONLY_DONE marker missing in $leader_log (mode $mode)" >&2
        tail -n 50 "$leader_log" || true
        return 1
    fi

    local summary_line
    summary_line=$(grep "FAST_RO_SUMMARY" "$leader_log" | tail -n 1 || true)
    if [ -z "$summary_line" ]; then
        echo "FAST_RO_SUMMARY line missing in $leader_log (mode $mode)" >&2
        tail -n 50 "$leader_log" || true
        return 1
    fi

    echo "$summary_line"
}

run_mode baseline
run_mode fast
