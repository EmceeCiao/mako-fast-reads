#!/bin/bash
set -euo pipefail

NSHARDS=${NSHARDS:-1}
SHARD_INDEX=${SHARD_INDEX:-0}
THREADS=${THREADS:-6}
RUNTIME=${RUNTIME:-60}
RESULT_DIR=${RESULT_DIR:-results/tpcc_fast_path_eval}

# Ensure env vars are exported (may be empty)
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-}"
export MAKO_FAST_RO_MODE="${MAKO_FAST_RO_MODE:-}"

declare -a SHARD_PIDS=()

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

cleanup_background() {
    pkill -9 -f "dbtest.*shard-index ${SHARD_INDEX}" 2>/dev/null || true
    pkill -9 -f simplePaxos 2>/dev/null || true
    pkill -9 -f "bash/shard.sh" 2>/dev/null || true
    if [ -n "${SHARD_PIDS[*]:-}" ]; then
        for pid in "${SHARD_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        done
    fi
}
trap cleanup_background EXIT

cleanup_previous_state() {
    pkill -9 -f dbtest 2>/dev/null || true
    pkill -9 -f simplePaxos 2>/dev/null || true
    pkill -9 -f "bash/shard.sh" 2>/dev/null || true
    rm -f nfs_sync_* 2>/dev/null || true
    USERNAME=${USER:-unknown}
    rm -rf /tmp/${USERNAME}_mako_rocksdb_shard* 2>/dev/null || true
}

mkdir -p "$RESULT_DIR"
cleanup_previous_state

log "Starting TPCC fast-path evaluation"
log "Results directory: $RESULT_DIR"
log "NSHARDS=$NSHARDS SHARD_INDEX=$SHARD_INDEX THREADS=$THREADS RUNTIME=${RUNTIME}s"
log "MAKO_TPCC_WORKLOAD_MIX=${MAKO_TPCC_WORKLOAD_MIX:-default}"
log "MAKO_FAST_RO_MODE=${MAKO_FAST_RO_MODE:-baseline}"

start_shard_proc() {
    local cluster_name=$1
    local log_file=$2
    log "Launching shard ${SHARD_INDEX} (${cluster_name}) -> $log_file"
    nohup bash bash/shard.sh "$NSHARDS" "$SHARD_INDEX" "$THREADS" "$cluster_name" 0 1 >"$log_file" 2>&1 &
    SHARD_PIDS+=($!)
}

leader_log="${RESULT_DIR}/shard${SHARD_INDEX}-localhost-${THREADS}.log"
learner_log="${RESULT_DIR}/shard${SHARD_INDEX}-learner-${THREADS}.log"
p2_log="${RESULT_DIR}/shard${SHARD_INDEX}-p2-${THREADS}.log"
p1_log="${RESULT_DIR}/shard${SHARD_INDEX}-p1-${THREADS}.log"

start_shard_proc localhost "$leader_log"
start_shard_proc learner "$learner_log"
start_shard_proc p2 "$p2_log"
sleep 1
start_shard_proc p1 "$p1_log"

log "Cluster started; sleeping for ${RUNTIME}s to gather throughput"
sleep "$RUNTIME"

log "Stopping dbtest cluster"
cleanup_background

log "Extracting throughput from $leader_log"
if [ ! -f "$leader_log" ]; then
    echo "ERROR: Leader log $leader_log not found" >&2
    exit 1
fi

throughput_line=$(grep "agg_persist_throughput" "$leader_log" | tail -n 1 || true)
if [ -z "$throughput_line" ]; then
    echo "ERROR: agg_persist_throughput not found in $leader_log" >&2
    exit 1
fi

throughput=$(echo "$throughput_line" | awk '{print $(NF-1)}')

log "agg_persist_throughput=${throughput}"
echo "EVAL_RESULT workload=tpcc_80_20 nshards=$NSHARDS shard=${SHARD_INDEX} threads=$THREADS runtime_s=$RUNTIME mix=${MAKO_TPCC_WORKLOAD_MIX:-default} fast_ro_mode=${MAKO_FAST_RO_MODE:-baseline} agg_persist_throughput=${throughput}"

exit 0
