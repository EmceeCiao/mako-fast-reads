#!/bin/bash

#############################################################################
# Multi-Shard Follower Read Evaluation Script
#
# Based on the proven test_2shard_replication.sh pattern for reliability.
# Supports 2, 3, or 4 shards with explicit PID tracking.
#############################################################################

echo "========================================="
echo "Multi-Shard Follower Read Evaluation"
echo "========================================="

# Configuration
NSHARDS=${NSHARDS:-3}          # Default 3 shards (12 processes × 2 threads = 24 cores)
THREADS=${THREADS:-2}          # Threads per replica
RUNTIME=${RUNTIME:-60}         # Benchmark runtime in seconds
RESULT_DIR=${RESULT_DIR:-results/multishard_follower_eval}

# Workload: 80% read-only (OrderStatus + StockLevel)
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-10,10,0,40,40}"
export MAKO_FAST_RO_MODE="${MAKO_FAST_RO_MODE:-fast}"

# Cleanup old state
rm -f nfs_sync_*
USERNAME=${USER:-unknown}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
ps aux | grep -i simplePaxos | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
pkill -9 -f "bash/shard.sh" 2>/dev/null || true
sleep 2

mkdir -p "$RESULT_DIR"

echo "Configuration:"
echo "  NSHARDS: $NSHARDS"
echo "  THREADS: $THREADS"
echo "  Total processes: $((NSHARDS * 4))"
echo "  Total cores: $((NSHARDS * 4 * THREADS))"
echo "  RUNTIME: ${RUNTIME}s"
echo "  WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX"
echo "  FAST_RO_MODE: $MAKO_FAST_RO_MODE"
echo "========================================="

# Ensure Paxos configs exist
for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    paxos_file="config/1leader_2followers/paxos${THREADS}_shardidx${shard_idx}.yml"
    if [ ! -f "$paxos_file" ]; then
        echo "Generating Paxos configs..."
        (cd config/1leader_2followers && python3 generator.py)
        break
    fi
done

# Arrays to track PIDs
declare -a ALL_PIDS=()

# Start each shard with all 4 replicas
for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    echo "Starting shard $shard_idx..."

    nohup bash bash/shard.sh $NSHARDS $shard_idx $THREADS localhost 0 1 > ${RESULT_DIR}/shard${shard_idx}-localhost.log 2>&1 &
    ALL_PIDS+=($!)

    nohup bash bash/shard.sh $NSHARDS $shard_idx $THREADS learner 0 1 > ${RESULT_DIR}/shard${shard_idx}-learner.log 2>&1 &
    ALL_PIDS+=($!)

    nohup bash bash/shard.sh $NSHARDS $shard_idx $THREADS p2 0 1 > ${RESULT_DIR}/shard${shard_idx}-p2.log 2>&1 &
    ALL_PIDS+=($!)

    sleep 1

    nohup bash bash/shard.sh $NSHARDS $shard_idx $THREADS p1 0 1 > ${RESULT_DIR}/shard${shard_idx}-p1.log 2>&1 &
    ALL_PIDS+=($!)

    sleep 2
done

echo "Started ${#ALL_PIDS[@]} processes"
echo "PIDs: ${ALL_PIDS[*]}"

# Wait for benchmark to start (check leader logs)
echo "Waiting for clusters to initialize..."
max_wait=180
waited=0
interval=5
all_ready=false

while [ $waited -lt $max_wait ]; do
    ready_count=0
    for shard_idx in $(seq 0 $((NSHARDS - 1))); do
        log_file="${RESULT_DIR}/shard${shard_idx}-localhost.log"
        if [ -f "$log_file" ] && grep -q "starting benchmark" "$log_file"; then
            ready_count=$((ready_count + 1))
        fi
    done

    if [ $ready_count -eq $NSHARDS ]; then
        echo "All $NSHARDS shards ready!"
        all_ready=true
        break
    fi

    echo "  $ready_count/$NSHARDS shards ready, waiting..."
    sleep $interval
    waited=$((waited + interval))
done

if [ "$all_ready" = false ]; then
    echo "WARNING: Not all shards became ready within ${max_wait}s"
fi

# Run benchmark
echo "Running benchmark for ${RUNTIME}s..."
sleep $RUNTIME

# Stop all processes - FORCE KILL (following test_2shard_replication.sh pattern)
echo "Stopping all processes..."

# Kill bash/shard.sh first to prevent respawning
pkill -9 -f "bash/shard.sh" 2>/dev/null || true

# Kill all dbtest processes
pkill -9 dbtest 2>/dev/null || true
killall -9 dbtest 2>/dev/null || true

sleep 2

# Check for remaining processes
remaining=$(ps aux | grep "dbtest" | grep -v grep | wc -l)
if [ "$remaining" -gt 0 ]; then
    echo "WARNING: $remaining dbtest processes still present"
    pids=$(ps aux | grep "dbtest" | grep -v grep | awk '{print $2}')
    for pid in $pids; do
        echo "Force killing PID $pid"
        kill -9 $pid 2>/dev/null || true
    done
    sleep 1
fi

# Reap zombie processes
for pid in "${ALL_PIDS[@]}"; do
    wait $pid 2>/dev/null || true
done

# Collect results
echo ""
echo "========================================="
echo "Results"
echo "========================================="

failed=0
total_throughput=0

for shard_idx in $(seq 0 $((NSHARDS - 1))); do
    log_file="${RESULT_DIR}/shard${shard_idx}-localhost.log"
    echo ""
    echo "Shard $shard_idx:"

    if [ ! -f "$log_file" ]; then
        echo "  ERROR: Log file not found"
        failed=1
        continue
    fi

    if grep -q "agg_persist_throughput" "$log_file"; then
        throughput_line=$(grep "agg_persist_throughput" "$log_file" | tail -1)
        throughput=$(echo "$throughput_line" | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9.]+$/) print $i}' | tail -1)
        echo "  Throughput: $throughput ops/sec"

        if [ -n "$throughput" ]; then
            total_throughput=$(echo "$total_throughput + $throughput" | bc -l 2>/dev/null || echo "$throughput")
        fi
    else
        echo "  ERROR: No throughput data found"
        echo "  Last 10 lines:"
        tail -10 "$log_file" | sed 's/^/    /'
        failed=1
    fi
done

echo ""
echo "========================================="
echo "TOTAL THROUGHPUT: $total_throughput ops/sec"
echo "========================================="

# Write summary
cat > "${RESULT_DIR}/summary.txt" << EOF
Multi-Shard Follower Read Evaluation
=====================================
NSHARDS: $NSHARDS
THREADS: $THREADS
RUNTIME: ${RUNTIME}s
WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX
FAST_RO_MODE: $MAKO_FAST_RO_MODE

TOTAL: $total_throughput ops/sec
EOF

echo "Summary: ${RESULT_DIR}/summary.txt"

# Final cleanup
ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
ps aux | grep -i simplePaxos | awk "{print \$2}" | xargs kill -9 2>/dev/null || true

# Output for CI parsing
echo "EVAL_RESULT nshards=$NSHARDS threads=$THREADS runtime_s=$RUNTIME mix=${MAKO_TPCC_WORKLOAD_MIX} fast_ro_mode=${MAKO_FAST_RO_MODE} total_throughput=${total_throughput}"

if [ $failed -eq 0 ]; then
    exit 0
else
    exit 1
fi
