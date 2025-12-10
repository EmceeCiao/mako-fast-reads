#!/usr/bin/env bash
set -euo pipefail

#############################################################################
# Multi-Shard Follower Read Evaluation Script
#
# Demonstrates the performance benefits of follower reads across multiple shards.
#
# Key metrics:
# - Aggregate throughput across all shards
# - Read scalability (more followers = more read capacity)
# - Comparison between baseline and fast-path modes
#
# Resource requirements for 32 cores / 64GB RAM:
# - 2 shards x 4 replicas = 8 processes
# - 4 threads per process = 32 total worker threads
# - ~6GB memory per replica = ~48GB total
#############################################################################

# Configuration (tune for your machine)
NSHARDS=${NSHARDS:-4}          # Default to 4 shards for better multi-shard demonstration
NREPLICAS=${NREPLICAS:-4}      # Replicas per shard (localhost, p1, p2, learner)
THREADS=${THREADS:-2}          # threads per replica (2 threads x 16 processes = 32 cores for 4 shards)
RUNTIME=${RUNTIME:-60}
RESULT_DIR=${RESULT_DIR:-results/multishard_follower_eval}
WAIT_FOR_METRIC_SECS=${WAIT_FOR_METRIC_SECS:-30}
FOLLOWER_READY_TIMEOUT=${FOLLOWER_READY_TIMEOUT:-180}

# For 32-core machines:
# - 4 shards x 4 replicas = 16 processes x 2 threads = 32 cores
# - 3 shards x 4 replicas = 12 processes x 2-3 threads = 24-36 cores
# Adjust NSHARDS and THREADS based on available cores

# Workload mix: NewOrder, Payment, Delivery, OrderStatus, StockLevel
# Default: 80% read-only (OrderStatus + StockLevel) to demonstrate follower read benefits
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-10,10,0,40,40}"
export MAKO_FAST_RO_MODE="${MAKO_FAST_RO_MODE:-fast}"

declare -a ALL_PIDS=()

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
    sleep 2
}

graceful_stop_all() {
    log "Stopping all dbtest processes gracefully..."
    pkill -2 -f dbtest 2>/dev/null || true
    local waited=0
    local timeout=30
    while pgrep -f dbtest >/dev/null 2>&1 && [ $waited -lt $timeout ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if pgrep -f dbtest >/dev/null 2>&1; then
        log "Force killing remaining dbtest processes"
        pkill -9 -f dbtest 2>/dev/null || true
    fi
}

cleanup_background() {
    graceful_stop_all
    pkill -9 -f simplePaxos 2>/dev/null || true
    pkill -9 -f "bash/shard.sh" 2>/dev/null || true
    if [ "${#ALL_PIDS[@]}" -gt 0 ]; then
        for pid in "${ALL_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        done
    fi
}
trap cleanup_background EXIT

ensure_paxos_configs() {
    for shard_idx in $(seq 0 $((NSHARDS - 1))); do
        local paxos_file="config/1leader_2followers/paxos${THREADS}_shardidx${shard_idx}.yml"
        if [ ! -f "$paxos_file" ]; then
            log "Generating Paxos configs..."
            (cd config/1leader_2followers && python3 generator.py)
            break
        fi
    done
}

start_shard_replica() {
    local shard_idx=$1
    local cluster_name=$2
    local log_file=$3
    log "Starting shard $shard_idx replica ($cluster_name) -> $log_file"
    nohup bash bash/shard.sh "$NSHARDS" "$shard_idx" "$THREADS" "$cluster_name" 0 1 >"$log_file" 2>&1 &
    ALL_PIDS+=($!)
}

wait_for_replica_ready() {
    local log_file=$1
    local label=$2
    local waited=0
    local interval=5
    while [ $waited -lt $FOLLOWER_READY_TIMEOUT ]; do
        if [ -f "$log_file" ]; then
            if grep -q "starting benchmark" "$log_file"; then
                log "Replica $label ready"
                return 0
            fi
        fi
        sleep "$interval"
        waited=$((waited + interval))
    done
    log "WARNING: Replica $label did not become ready within ${FOLLOWER_READY_TIMEOUT}s"
    return 1
}

extract_throughput() {
    local log_file=$1
    local attempts=0
    local interval=2
    local max_attempts=$((WAIT_FOR_METRIC_SECS / interval))
    [ $max_attempts -lt 1 ] && max_attempts=1
    while [ $attempts -lt $max_attempts ]; do
        if [ -f "$log_file" ]; then
            local line
            line=$(grep "agg_persist_throughput" "$log_file" | tail -n 1 || true)
            if [ -n "$line" ]; then
                echo "$line" | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9.]+$/) print $i}' | tail -1
                return 0
            fi
        fi
        attempts=$((attempts + 1))
        sleep "$interval"
    done
    echo "0"
}

extract_fast_path_stats() {
    local log_file=$1
    if [ -f "$log_file" ]; then
        grep -E "(fast_path_attempts|fast_path_successes|follower_reads)" "$log_file" | tail -5 || true
    fi
}

#############################################################################
# Main execution
#############################################################################

mkdir -p "$RESULT_DIR"
cleanup_previous_state

TOTAL_PROCESSES=$((NSHARDS * NREPLICAS))
TOTAL_CORES=$((TOTAL_PROCESSES * THREADS))

log "=============================================="
log "Multi-Shard Follower Read Evaluation"
log "=============================================="
log "Configuration:"
log "  NSHARDS=$NSHARDS"
log "  NREPLICAS=$NREPLICAS (per shard)"
log "  THREADS=$THREADS (per replica)"
log "  Total processes: $TOTAL_PROCESSES"
log "  Total cores used: $TOTAL_CORES"
log "  RUNTIME=${RUNTIME}s"
log "  WORKLOAD_MIX=${MAKO_TPCC_WORKLOAD_MIX}"
log "  FAST_RO_MODE=${MAKO_FAST_RO_MODE}"
log "=============================================="

# Validate configuration
if [ "$TOTAL_CORES" -gt 64 ]; then
    log "WARNING: Configuration uses $TOTAL_CORES cores, which may exceed available resources"
fi

ensure_paxos_configs

# Start all shards
for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    log "Starting shard $shard_idx (4 replicas)..."

    start_shard_replica "$shard_idx" "localhost" "${RESULT_DIR}/shard${shard_idx}-localhost.log"
    start_shard_replica "$shard_idx" "learner"   "${RESULT_DIR}/shard${shard_idx}-learner.log"
    start_shard_replica "$shard_idx" "p2"        "${RESULT_DIR}/shard${shard_idx}-p2.log"
    sleep 1
    start_shard_replica "$shard_idx" "p1"        "${RESULT_DIR}/shard${shard_idx}-p1.log"

    sleep 2  # Allow startup before next shard
done

# Wait for all replicas to be ready
log "Waiting for all replicas to be ready..."
for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    for replica in localhost learner p1 p2; do
        wait_for_replica_ready "${RESULT_DIR}/shard${shard_idx}-${replica}.log" "shard${shard_idx}-${replica}" || true
    done
done

log "All replicas started. Running benchmark for ${RUNTIME}s..."
sleep "$RUNTIME"

log "Stopping cluster..."
graceful_stop_all

# Allow time for final metrics to be written
sleep 5

#############################################################################
# Results collection
#############################################################################

log ""
log "=============================================="
log "Results Summary"
log "=============================================="

total_throughput=0
declare -a shard_throughputs=()

for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    log_file="${RESULT_DIR}/shard${shard_idx}-localhost.log"
    throughput=$(extract_throughput "$log_file")

    if [ -n "$throughput" ] && [ "$throughput" != "0" ]; then
        shard_throughputs+=("$throughput")
        total_throughput=$(echo "$total_throughput + $throughput" | bc -l 2>/dev/null || echo "$throughput")
        log "Shard $shard_idx throughput: $throughput ops/sec"

        # Show fast path stats if available
        fast_stats=$(extract_fast_path_stats "$log_file")
        if [ -n "$fast_stats" ]; then
            log "  Fast path stats: $fast_stats"
        fi
    else
        log "WARNING: No throughput found for shard $shard_idx"
        if [ -f "$log_file" ]; then
            log "  Last 10 lines of log:"
            tail -10 "$log_file" | sed 's/^/    /'
        fi
    fi
done

log ""
log "=============================================="
log "TOTAL AGGREGATE THROUGHPUT: $total_throughput ops/sec"
log "=============================================="

# Generate summary output
summary_file="${RESULT_DIR}/summary.txt"
{
    echo "Multi-Shard Follower Read Evaluation Results"
    echo "=============================================="
    echo "Configuration:"
    echo "  NSHARDS: $NSHARDS"
    echo "  THREADS_PER_REPLICA: $THREADS"
    echo "  TOTAL_PROCESSES: $((NSHARDS * 4))"
    echo "  RUNTIME: ${RUNTIME}s"
    echo "  WORKLOAD_MIX: ${MAKO_TPCC_WORKLOAD_MIX}"
    echo "  FAST_RO_MODE: ${MAKO_FAST_RO_MODE}"
    echo ""
    echo "Results:"
    for i in "${!shard_throughputs[@]}"; do
        echo "  Shard $i: ${shard_throughputs[$i]} ops/sec"
    done
    echo ""
    echo "TOTAL: $total_throughput ops/sec"
    echo ""
    echo "Per-shard average: $(echo "scale=2; $total_throughput / $NSHARDS" | bc -l 2>/dev/null || echo "N/A") ops/sec"
} > "$summary_file"

log "Summary written to: $summary_file"

# Final output line for automated parsing
echo "EVAL_RESULT workload=multishard_follower nshards=$NSHARDS threads=$THREADS runtime_s=$RUNTIME mix=${MAKO_TPCC_WORKLOAD_MIX} fast_ro_mode=${MAKO_FAST_RO_MODE} total_throughput=${total_throughput}"
