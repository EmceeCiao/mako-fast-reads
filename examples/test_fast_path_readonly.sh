#!/bin/bash

set -euo pipefail

echo "========================================="
echo "Testing fast-path read-only workload in replicated mode"
echo "========================================="

pkill -9 -f fastPathReadOnly 2>/dev/null || true
rm -f fastpath-shard0*.log nfs_sync_* || true
USERNAME=${USER:-unknown}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard* || true

start_instance() {
    local role="$1"
    local logfile="fastpath-shard0-${role}.log"
    echo "Starting shard role ${role}..."
    nohup ./build/fastPathReadOnly 1 0 4 "${role}" 1 > "${logfile}" 2>&1 &
    echo $!
}

PID_LOCALHOST=$(start_instance localhost)
PID_LEARNER=$(start_instance learner)
PID_P2=$(start_instance p2)
sleep 1
PID_P1=$(start_instance p1)

sleep 30

echo "Stopping fast-path read-only processes..."
kill $PID_P1 $PID_P2 $PID_LEARNER $PID_LOCALHOST 2>/dev/null || true
wait $PID_P1 $PID_P2 $PID_LEARNER $PID_LOCALHOST 2>/dev/null || true

LOGFILE="fastpath-shard0-p1.log"
LEADER_LOG="fastpath-shard0-localhost.log"
if [ ! -f "$LEADER_LOG" ]; then
    echo "Leader log $LEADER_LOG not found"
    exit 1
fi

if grep -q "FAST_PATH_READONLY_DONE" "$LEADER_LOG"; then
    echo "Fast-path read-only workload completed successfully."
else
    echo "FAST_PATH_READONLY_DONE marker not found in $LEADER_LOG"
    tail -20 "$LEADER_LOG"
    exit 1
fi

echo ""
echo "---- FAST READ-ONLY METRICS (from $LEADER_LOG) ----"
grep "FAST_RO_STATS" "$LEADER_LOG" || echo "No FAST_RO_STATS line found"

echo "========================================="
echo "Fast-path read-only test PASSED"
echo "========================================="
