#!/usr/bin/env bash
set -euo pipefail

#############################################################################
# Follower Read Benefit Comparison Script
#
# Runs the same workload twice:
# 1. Baseline mode (no fast path) - all reads go to leader
# 2. Fast-path mode - read-only transactions can be served by followers
#
# Then compares the throughput to demonstrate follower read benefits.
#
# Ideal for demonstrating:
# - Read scalability: N followers can serve N times the read load
# - Latency improvement: Local reads avoid network round-trip to leader
#############################################################################

NSHARDS=${NSHARDS:-4}
NREPLICAS=${NREPLICAS:-4}
THREADS=${THREADS:-2}          # 4 shards x 4 replicas x 2 threads = 32 cores
RUNTIME=${RUNTIME:-60}
RESULT_BASE_DIR=${RESULT_BASE_DIR:-results/follower_read_comparison}

# 80% read-only workload to maximize follower read benefit
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-10,10,0,40,40}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

run_evaluation() {
    local mode=$1
    local result_dir="${RESULT_BASE_DIR}/${mode}"

    log "=============================================="
    log "Running evaluation: MODE=$mode"
    log "=============================================="

    export MAKO_FAST_RO_MODE="$mode"
    export RESULT_DIR="$result_dir"
    export NSHARDS="$NSHARDS"
    export NREPLICAS="$NREPLICAS"
    export THREADS="$THREADS"
    export RUNTIME="$RUNTIME"

    mkdir -p "$result_dir"

    # Run the evaluation script
    if ! bash scripts/run_multishard_follower_read_eval.sh; then
        log "WARNING: Evaluation for mode=$mode may have had issues"
    fi

    # Extract the result
    local summary="${result_dir}/summary.txt"
    if [ -f "$summary" ]; then
        local throughput
        throughput=$(grep "^TOTAL:" "$summary" | awk '{print $2}' || echo "0")
        echo "$throughput"
    else
        echo "0"
    fi
}

extract_result() {
    local result_dir=$1
    local summary="${result_dir}/summary.txt"
    if [ -f "$summary" ]; then
        grep "^TOTAL:" "$summary" | awk '{print $2}' || echo "0"
    else
        echo "0"
    fi
}

#############################################################################
# Main
#############################################################################

TOTAL_PROCESSES=$((NSHARDS * NREPLICAS))
TOTAL_CORES=$((TOTAL_PROCESSES * THREADS))

log "=============================================="
log "Follower Read Benefit Comparison"
log "=============================================="
log "Configuration:"
log "  NSHARDS: $NSHARDS"
log "  NREPLICAS: $NREPLICAS (per shard)"
log "  THREADS: $THREADS (per replica)"
log "  Total processes: $TOTAL_PROCESSES"
log "  Total cores: $TOTAL_CORES"
log "  RUNTIME: ${RUNTIME}s"
log "  WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX"
log "=============================================="

mkdir -p "$RESULT_BASE_DIR"

# Run baseline first
log ""
log ">>> PHASE 1: Running BASELINE mode (all reads to leader)"
run_evaluation "baseline" > /dev/null 2>&1 || true
baseline_throughput=$(extract_result "${RESULT_BASE_DIR}/baseline")
log "Baseline throughput: $baseline_throughput ops/sec"

# Cool down between runs
log ""
log "Cooling down for 10 seconds..."
sleep 10

# Run fast-path mode
log ""
log ">>> PHASE 2: Running FAST-PATH mode (follower reads enabled)"
run_evaluation "fast" > /dev/null 2>&1 || true
fast_throughput=$(extract_result "${RESULT_BASE_DIR}/fast")
log "Fast-path throughput: $fast_throughput ops/sec"

#############################################################################
# Results comparison
#############################################################################

log ""
log "=============================================="
log "COMPARISON RESULTS"
log "=============================================="

# Calculate improvement
if [ -n "$baseline_throughput" ] && [ "$baseline_throughput" != "0" ] && [ -n "$fast_throughput" ]; then
    improvement=$(echo "scale=2; (($fast_throughput - $baseline_throughput) / $baseline_throughput) * 100" | bc -l 2>/dev/null || echo "N/A")
    speedup=$(echo "scale=2; $fast_throughput / $baseline_throughput" | bc -l 2>/dev/null || echo "N/A")
else
    improvement="N/A"
    speedup="N/A"
fi

log ""
log "  BASELINE (all reads to leader):"
log "    Throughput: $baseline_throughput ops/sec"
log ""
log "  FAST-PATH (follower reads enabled):"
log "    Throughput: $fast_throughput ops/sec"
log ""
log "  IMPROVEMENT:"
log "    Speedup: ${speedup}x"
log "    Percent improvement: ${improvement}%"
log ""
log "=============================================="

# Write comparison summary
comparison_file="${RESULT_BASE_DIR}/comparison_summary.txt"
{
    echo "Follower Read Benefit Comparison"
    echo "================================"
    echo "Date: $(date)"
    echo ""
    echo "Configuration:"
    echo "  NSHARDS: $NSHARDS"
    echo "  THREADS: $THREADS"
    echo "  RUNTIME: ${RUNTIME}s"
    echo "  WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX (80% read-only)"
    echo ""
    echo "Results:"
    echo "  Baseline throughput: $baseline_throughput ops/sec"
    echo "  Fast-path throughput: $fast_throughput ops/sec"
    echo "  Speedup: ${speedup}x"
    echo "  Improvement: ${improvement}%"
    echo ""
    echo "Expected benefits of follower reads:"
    echo "  - With 3 followers per shard, read capacity can scale ~3x"
    echo "  - Read-only transactions avoid 2PC coordination overhead"
    echo "  - Local reads reduce network latency"
} > "$comparison_file"

log "Comparison summary: $comparison_file"
log ""

# Machine-readable output
echo "COMPARISON_RESULT nshards=$NSHARDS threads=$THREADS runtime_s=$RUNTIME mix=${MAKO_TPCC_WORKLOAD_MIX} baseline_throughput=${baseline_throughput} fast_throughput=${fast_throughput} speedup=${speedup} improvement_pct=${improvement}"
