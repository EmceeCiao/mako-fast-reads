#!/bin/bash

#############################################################################
# Follower Read Benefit Comparison Script
#
# Runs the same workload twice:
# 1. Baseline mode (no fast path) - all reads go to leader
# 2. Fast-path mode - read-only transactions can be served by followers
#############################################################################

echo "========================================="
echo "Follower Read Benefit Comparison"
echo "========================================="

NSHARDS=${NSHARDS:-3}
THREADS=${THREADS:-2}
RUNTIME=${RUNTIME:-60}
RESULT_BASE_DIR=${RESULT_BASE_DIR:-results/follower_read_comparison}

# 80% read-only workload
export MAKO_TPCC_WORKLOAD_MIX="${MAKO_TPCC_WORKLOAD_MIX:-10,10,0,40,40}"

echo "Configuration:"
echo "  NSHARDS: $NSHARDS"
echo "  THREADS: $THREADS"
echo "  Total processes: $((NSHARDS * 4))"
echo "  Total cores: $((NSHARDS * 4 * THREADS))"
echo "  RUNTIME: ${RUNTIME}s"
echo "  WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX"
echo "========================================="

mkdir -p "$RESULT_BASE_DIR"

extract_total_throughput() {
    local result_dir=$1
    local summary="${result_dir}/summary.txt"
    if [ -f "$summary" ]; then
        grep "^TOTAL:" "$summary" | awk '{print $2}' || echo "0"
    else
        echo "0"
    fi
}

#############################################################################
# Run BASELINE
#############################################################################

echo ""
echo ">>> PHASE 1: Running BASELINE mode (all reads to leader)"
echo "========================================="

export MAKO_FAST_RO_MODE=""
export RESULT_DIR="${RESULT_BASE_DIR}/baseline"

bash scripts/run_multishard_follower_read_eval.sh

baseline_throughput=$(extract_total_throughput "$RESULT_DIR")
echo "Baseline throughput: $baseline_throughput ops/sec"

# Cool down
echo ""
echo "Cooling down for 15 seconds..."
sleep 15

#############################################################################
# Run FAST-PATH
#############################################################################

echo ""
echo ">>> PHASE 2: Running FAST-PATH mode (follower reads enabled)"
echo "========================================="

export MAKO_FAST_RO_MODE="fast"
export RESULT_DIR="${RESULT_BASE_DIR}/fast"

bash scripts/run_multishard_follower_read_eval.sh

fast_throughput=$(extract_total_throughput "$RESULT_DIR")
echo "Fast-path throughput: $fast_throughput ops/sec"

#############################################################################
# Compare Results
#############################################################################

echo ""
echo "========================================="
echo "COMPARISON RESULTS"
echo "========================================="

# Calculate improvement
if [ -n "$baseline_throughput" ] && [ "$baseline_throughput" != "0" ] && [ -n "$fast_throughput" ]; then
    improvement=$(echo "scale=2; (($fast_throughput - $baseline_throughput) / $baseline_throughput) * 100" | bc -l 2>/dev/null || echo "N/A")
    speedup=$(echo "scale=2; $fast_throughput / $baseline_throughput" | bc -l 2>/dev/null || echo "N/A")
else
    improvement="N/A"
    speedup="N/A"
fi

echo ""
echo "  BASELINE (all reads to leader):"
echo "    Throughput: $baseline_throughput ops/sec"
echo ""
echo "  FAST-PATH (follower reads enabled):"
echo "    Throughput: $fast_throughput ops/sec"
echo ""
echo "  IMPROVEMENT:"
echo "    Speedup: ${speedup}x"
echo "    Percent improvement: ${improvement}%"
echo ""
echo "========================================="

# Write comparison summary
cat > "${RESULT_BASE_DIR}/comparison_summary.txt" << EOF
Follower Read Benefit Comparison
================================
Date: $(date)

Configuration:
  NSHARDS: $NSHARDS
  THREADS: $THREADS
  RUNTIME: ${RUNTIME}s
  WORKLOAD_MIX: $MAKO_TPCC_WORKLOAD_MIX (80% read-only)

Results:
  Baseline throughput: $baseline_throughput ops/sec
  Fast-path throughput: $fast_throughput ops/sec
  Speedup: ${speedup}x
  Improvement: ${improvement}%
EOF

echo "Summary: ${RESULT_BASE_DIR}/comparison_summary.txt"

# Machine-readable output
echo "COMPARISON_RESULT nshards=$NSHARDS threads=$THREADS runtime_s=$RUNTIME mix=${MAKO_TPCC_WORKLOAD_MIX} baseline_throughput=${baseline_throughput} fast_throughput=${fast_throughput} speedup=${speedup} improvement_pct=${improvement}"
