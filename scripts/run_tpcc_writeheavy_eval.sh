#!/usr/bin/env bash
set -euo pipefail

# Write-heavy TPC-C evaluation script
# Runs 80% write, 20% read (opposite of default 80/20 mix)
# Compares fast-path vs baseline performance on write-heavy workload

NSHARDS=${NSHARDS:-1}
SHARD_INDEX=${SHARD_INDEX:-0}
THREADS=${THREADS:-6}
RUNTIME=${RUNTIME:-60}
RESULT_DIR=${RESULT_DIR:-results/tpcc_writeheavy_eval}
WAIT_FOR_METRIC_SECS=${WAIT_FOR_METRIC_SECS:-30}
FOLLOWER_READY_TIMEOUT=${FOLLOWER_READY_TIMEOUT:-120}

# Write-heavy workload: 80% writes, 20% reads
# 40% NewOrder, 40% Payment, 0% Delivery, 10% OrderStatus, 10% StockLevel
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-40,40,0,10,10}"
export MAKO_FAST_RO_MODE="${MAKO_FAST_RO_MODE:-}"

declare -a SHARD_PIDS=()

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

cleanup_previous_state() {
    pkill -9 -f dbtest 2>/dev/null || true
    pkill -9 -f simplePaxos 2>/dev/null || true
    pkill -9 -f "bash/shard.sh" 2>/dev/null || true
    rm -f nfs_sync_* 2>/dev/null || true
    USERNAME=${USER:-unknown}
    rm -rf "/tmp/${USERNAME}_mako_rocksdb_shard"* 2>/dev/null || true
}

graceful_stop_dbtest() {
    local pattern="dbtest.*shard-index ${SHARD_INDEX}"
    if pgrep -f "$pattern" >/dev/null 2>&1; then
        log "Sending SIGINT to dbtest for graceful shutdown"
        pkill -2 -f "$pattern" 2>/dev/null || true
        local waited=0
        local timeout=20
        while pgrep -f "$pattern" >/dev/null 2>&1 && [ $waited -lt $timeout ]; do
            sleep 1
            waited=$((waited + 1))
        done
        if pgrep -f "$pattern" >/dev/null 2>&1; then
            log "dbtest still running, forcing shutdown"
            pkill -9 -f "$pattern" 2>/dev/null || true
        fi
    fi
}

cleanup_background() {
    graceful_stop_dbtest
    pkill -9 -f simplePaxos 2>/dev/null || true
    pkill -9 -f "bash/shard.sh" 2>/dev/null || true
    if [ "${#SHARD_PIDS[@]}" -gt 0 ]; then
        for pid in "${SHARD_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        done
    fi
}
trap cleanup_background EXIT

mkdir -p "$RESULT_DIR"
cleanup_previous_state

log "Starting TPCC write-heavy evaluation"
log "Results directory: $RESULT_DIR"
log "NSHARDS=$NSHARDS SHARD_INDEX=$SHARD_INDEX THREADS=$THREADS RUNTIME=${RUNTIME}s"
log "MAKO_TPCC_WORKLOAD_MIX=${MAKO_TPCC_WORKLOAD_MIX}"
log "MAKO_FAST_RO_MODE=${MAKO_FAST_RO_MODE:-baseline}"

ensure_paxos_config() {
    local paxos_file="config/1leader_2followers/paxos${THREADS}_shardidx${SHARD_INDEX}.yml"
    if [ -f "$paxos_file" ]; then
        return
    fi
    log "Missing $paxos_file, generating Paxos configs via config/1leader_2followers/generator.py"
    (cd config/1leader_2followers && python3 generator.py)
    if [ ! -f "$paxos_file" ]; then
        echo "ERROR: Failed to generate $paxos_file" >&2
        exit 1
    fi
}

ensure_paxos_config

start_shard_proc() {
    local cluster_name=$1
    local log_file=$2
    log "Launching shard ${SHARD_INDEX} (${cluster_name}) -> $log_file"
    nohup bash bash/shard.sh "$NSHARDS" "$SHARD_INDEX" "$THREADS" "$cluster_name" 0 1 >"$log_file" 2>&1 &
    SHARD_PIDS+=($!)
}

wait_for_follower_ready() {
    local log_file=$1
    local label=$2
    local waited=0
    local interval=5
    local max_wait=$FOLLOWER_READY_TIMEOUT
    while [ $waited -lt $max_wait ]; do
        if [ -f "$log_file" ]; then
            if grep -q "starting benchmark" "$log_file"; then
                log "Follower $label ready (detected 'starting benchmark')."
                return 0
            fi
        fi
        sleep "$interval"
        waited=$((waited + interval))
    done
    log "Follower $label did not signal readiness within ${max_wait}s"
    return 1
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

log "Waiting for replicas to finish load/start benchmark (timeout=${FOLLOWER_READY_TIMEOUT}s)"
wait_for_follower_ready "$learner_log" "learner" || true
wait_for_follower_ready "$p1_log" "p1" || true
wait_for_follower_ready "$p2_log" "p2" || true

log "Cluster started; sleeping for ${RUNTIME}s to gather throughput"
sleep "$RUNTIME"

log "Stopping dbtest cluster"
graceful_stop_dbtest

extract_throughput() {
    local attempts=0
    local interval=2
    local max_attempts=$((WAIT_FOR_METRIC_SECS / interval))
    if [ $max_attempts -lt 1 ]; then
        max_attempts=1
    fi
    while [ $attempts -lt $max_attempts ]; do
        if [ ! -f "$leader_log" ]; then
            log "Leader log $leader_log not found yet, waiting..."
        else
            local line
            line=$(grep "agg_persist_throughput" "$leader_log" | tail -n 1 || true)
            if [ -n "$line" ]; then
                echo "$line"
                return 0
            fi
        fi
        attempts=$((attempts + 1))
        sleep "$interval"
    done
    return 1
}

metric_line=$(extract_throughput) || {
    echo "ERROR: agg_persist_throughput not found in $leader_log" >&2
    if [ -f "$leader_log" ]; then
        log "Last 20 lines of leader log for debugging:"
        tail -n 20 "$leader_log" >&2
    fi
    exit 1
}

throughput=$(echo "$metric_line" | awk '{print $(NF-1)}')
log "agg_persist_throughput=${throughput}"

echo "EVAL_RESULT workload=tpcc_writeheavy nshards=$NSHARDS shard=${SHARD_INDEX} threads=$THREADS runtime_s=$RUNTIME workload_mix=${MAKO_TPCC_WORKLOAD_MIX} fast_ro_mode=${MAKO_FAST_RO_MODE:-baseline} agg_persist_throughput=${throughput}"
