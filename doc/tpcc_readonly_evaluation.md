# TPC-C Read-Only Evaluation Guide

This guide explains how to run and evaluate the fast read-only transaction path using pure read-only TPC-C workloads.

## Overview

There are two evaluation scripts:

1. **`scripts/run_tpcc_fast_path_eval.sh`** - Mixed workload (80% reads, 20% writes)
   - Workload mix: 10% NewOrder, 10% Payment, 0% Delivery, 40% OrderStatus, 40% StockLevel
   - More realistic TPC-C workload
   - Shows fast path benefit in mixed environment

2. **`scripts/run_tpcc_readonly_eval.sh`** - Pure read-only workload (100% OrderStatus)
   - Workload mix: 0% NewOrder, 0% Payment, 0% Delivery, 100% OrderStatus, 0% StockLevel
   - Isolates fast path performance
   - Shows upper bound of fast path capability

## Running Locally

### Baseline (No Fast Path)

```bash
# Mixed workload (80/20)
NSHARDS=1 THREADS=6 RUNTIME=60 \
MAKO_TPCC_WORKLOAD_MIX="10,10,0,40,40" \
MAKO_FAST_RO_MODE=baseline \
bash scripts/run_tpcc_fast_path_eval.sh

# Pure read-only
NSHARDS=1 THREADS=6 RUNTIME=60 \
MAKO_FAST_RO_MODE=baseline \
bash scripts/run_tpcc_readonly_eval.sh
```

### Fast Path Mode

```bash
# Mixed workload (80/20)
NSHARDS=1 THREADS=6 RUNTIME=60 \
MAKO_TPCC_WORKLOAD_MIX="10,10,0,40,40" \
MAKO_FAST_RO_MODE=fast \
bash scripts/run_tpcc_fast_path_eval.sh

# Pure read-only
NSHARDS=1 THREADS=6 RUNTIME=60 \
MAKO_FAST_RO_MODE=fast \
bash scripts/run_tpcc_readonly_eval.sh
```

## Environment Variables

### Common Settings

- `NSHARDS` - Number of shards (default: 1)
- `THREADS` - Number of threads per shard (default: 6)
- `RUNTIME` - Duration in seconds (default: 60)
- `RESULT_DIR` - Directory to store logs (default: `results/tpcc_*_eval`)

### Workload-Specific

- `MAKO_TPCC_WORKLOAD_MIX` - Comma-separated transaction percentages: `NewOrder,Payment,Delivery,OrderStatus,StockLevel`
  - Must sum to 100
  - Example: `"10,10,0,40,40"` for 80% reads / 20% writes
  - Example: `"0,0,0,100,0"` for 100% OrderStatus reads
  - Example: `"0,0,0,0,100"` for 100% StockLevel reads

- `MAKO_FAST_RO_MODE` - Fast path mode
  - Leave blank or set to `baseline` for standard path
  - Set to `fast` for fast read-only path

## Results Interpretation

### Key Metrics

```
agg_persist_throughput:      Operations per second (higher is better)
agg_abort_rate:              Aborts per second (lower is better)
avg_latency:                 Average transaction latency in ms (lower is better)
avg_per_core_throughput:     Throughput per thread (useful for scaling analysis)
```

### Transaction-Specific Metrics

For each transaction type (NewOrder, Payment, OrderStatus, StockLevel):

- `*_local_commit_latency` - Latency when transaction succeeds
- `*_local_abort_latency` - Latency when transaction aborts
- `*_local_abort_ratio` - Percentage of transactions that abort

### Interpretation Example

**Mixed Workload (80/20) Results:**

```
Fast Path:
- agg_persist_throughput: 1.1093e+06 ops/sec
- agg_abort_rate: 33249.5 aborts/sec
- OrderStatus_abort_ratio: 0.00425323 (0.43%)
- StockLevel_abort_ratio: 0.0684681 (6.85%)

Baseline:
- agg_persist_throughput: 263078 ops/sec
- agg_abort_rate: 37.2275 aborts/sec
```

**Interpretation:**
- Fast path: 4.2x higher throughput
- Fast path: Higher abort rate (expected for snapshot isolation)
- StockLevel abort rate increases due to snapshot timestamp constraints
- OrderStatus abort rate remains low because it does fewer scans

## CI/CD Workflows

### Workflow: TPCC Fast-Path Evaluation

File: `.github/workflows/tpcc-fast-path-eval.yml`

Runs mixed workload (80/20 mix) comparison.

```bash
gh workflow run tpcc-fast-path-eval.yml -f fast_ro_mode=fast
```

### Workflow: TPCC Read-Only Evaluation

File: `.github/workflows/tpcc-readonly-eval.yml`

Runs pure read-only workload (100% OrderStatus).

```bash
gh workflow run tpcc-readonly-eval.yml -f fast_ro_mode=fast
```

## Customizing Workload Mixes

The workload mix format is: `NewOrder,Payment,Delivery,OrderStatus,StockLevel`

### Example Scenarios

**Heavy on scans (StockLevel):**
```bash
MAKO_TPCC_WORKLOAD_MIX="0,0,0,10,90"  # 90% StockLevel, 10% OrderStatus
```

**Light reads:**
```bash
MAKO_TPCC_WORKLOAD_MIX="40,40,10,5,5"  # 50% writes, 50% reads
```

**Balanced:**
```bash
MAKO_TPCC_WORKLOAD_MIX="25,25,25,12,13"  # ~50% writes, ~50% reads
```

## Scaling to Multiple Shards

To test with multiple shards, scale threads proportionally:

```bash
# 2 shards, 6 threads each (12 total)
NSHARDS=2 THREADS=12 RUNTIME=60 \
MAKO_FAST_RO_MODE=fast \
bash scripts/run_tpcc_readonly_eval.sh

# 4 shards, 6 threads each (24 total)
NSHARDS=4 THREADS=24 RUNTIME=60 \
MAKO_FAST_RO_MODE=fast \
bash scripts/run_tpcc_readonly_eval.sh
```

## Output Locations

Results are stored in `RESULT_DIR` (default: `results/tpcc_*_eval/`)

Each shard generates:
- `shard${SHARD_INDEX}-localhost-${THREADS}.log` - Leader log
- `shard${SHARD_INDEX}-learner-${THREADS}.log` - Learner replica log
- `shard${SHARD_INDEX}-p1-${THREADS}.log` - Paxos group 1 replica log
- `shard${SHARD_INDEX}-p2-${THREADS}.log` - Paxos group 2 replica log

## Troubleshooting

### "ALWAYS ERROR" Panic

If you see:
```
PANIC ALWAYS_ERROR (tpcc.cc:123): the error for ALWAYS ERROR!
```

This means scans are returning empty results and the fast path isn't handling it gracefully. Ensure:
1. Fast path changes are properly applied (see `doc/fix_tpcc_fast_read_plan.md`)
2. `MAKO_FAST_RO_MODE` is set correctly

### Missing Metrics

If `agg_persist_throughput` metric is missing:
1. Check if all replicas started successfully (look for "starting benchmark" in logs)
2. Increase `RUNTIME` to allow more time for metrics collection
3. Check leader log for errors or early shutdown

### High Abort Rates

High abort rates are normal for the fast path in mixed workloads. This is because:
- Snapshot isolation is stricter than baseline
- MVCC snapshot reads may not find versions at requested timestamp
- Replication lag can cause scans to fail

If abort rates are too high:
1. Try pure read workload (`0,0,0,100,0`) to isolate the issue
2. Check if watermark advancement is slow (check logs for "advancing watermark")
3. Consider increasing `RUNTIME` for better statistics

## Summary

| Workload | Mode | Expected Behavior | Use Case |
|----------|------|-------------------|----------|
| Mixed 80/20 | Baseline | Lower throughput, low abort rate | Production baseline |
| Mixed 80/20 | Fast | Higher throughput, higher abort rate | Production fast path |
| Pure Read | Baseline | Low throughput | Read-only baseline |
| Pure Read | Fast | Highest throughput, low abort rate | Read-only upper bound |

The fast path provides significant throughput improvements, especially for read-heavy workloads, with the trade-off of accepting higher abort rates due to snapshot isolation semantics.
