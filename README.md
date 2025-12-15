# Mako Fast Reads

<div align="center">

![CI](https://github.com/makodb/mako/actions/workflows/ci.yml/badge.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**High-Performance Distributed Transactional Key-Value Store with Geo-Replication Support**

</div>

---

## What is Mako Fast Reads? 

This repository is a fork of **Mako**, a high-performance distributed transactional key-value store system with geo-replication support, built on cutting-edge systems research. For more information on **Mako** please check out the original [repository]([https://github.com/makodb/mako]) and the paper submitted to [OSDI'25](https://www.usenix.org/conference/osdi25/presentation/shen-weihai). We thank the original developers of **Mako** for all their hard work.  

Within this repository we build upon **Mako** exploring and implementing a read-only fast path for **Mako** that executes read-only transactions on follower-safe snapshots selected via shard replication watermarks, bypassing 2PC while preserving serializability through optimistic validation and conservative fallback. This design trades snapshot freshness for performance without weakening correctness guarantees. Across TPC-C workloads, the fast path improves throughput by up to 4.2× and reduces latency by up to 4.6×, including multi-shard configurations, while increasing abort rates that remain a small fraction of total executions.

---

## What is the design behind Fast Read-Only Transaction Paths?

**Why a fast path:** Two-Phase Commit (2PC) provides atomic commit across machines, but it is excessive for **read-only** transactions because nothing is written or committed. In Mako, the baseline path can also force reads to wait until replication becomes safe, adding latency and creating leader-side bottlenecks.

**Core idea:** Choose a **follower-safe snapshot** *up front* using shard replication watermarks, then execute/validate the read-only transaction at that snapshot. If any safety condition is not met, **fall back** to the baseline (2PC) path. This trades snapshot freshness for performance while preserving correctness.

Our Fast read-only execution proceeds in three phases:

1. **Classify + fallback**
   - Execute reads normally to collect the read set.
   - If the transaction performs any writes: **use the baseline 2PC path**.
   - Otherwise: attempt the read-only fast path; fall back if prerequisites fail.

2. **Durability / global snapshot validation**
   - Fetch replication **watermarks** for the shards involved in the read set and take the **minimum** as the conservative snapshot timestamp.
   - Check that every fetched item’s timestamp is ≤ this snapshot (i.e., the read is within the follower-safe replicated prefix).
   - If the check fails: **fall back to baseline 2PC**.

3. **Consistency / optimistic validation**
   - Validate that each read item’s version still matches the version in the local Masstree (detecting concurrent updates).
   - If any mismatch is observed: **abort** (caller may retry).
   - If validation succeeds: complete without 2PC, releasing resources and cleaning up.

---

## Files We Changed 

Major changes in the repository were made in the following files: 
- **Fast read-only path / follower-safe reads (core transaction + watermark logic):**
  - `src/mako/benchmarks/sto/MassTrans.hh`
  - `src/mako/benchmarks/sto/Transaction.cc`
  - `src/mako/benchmarks/sto/Transaction.hh`
  - `src/mako/benchmarks/sto/TransactionStats.hh` (added)
  - `src/mako/benchmarks/sto/multiversion.hh`
  - `src/mako/benchmarks/sto/sync_util.hh`
  - `src/mako/benchmarks/sync_util_init.cc`
  - `src/mako/mako.hh`
  - `src/mako/txn.h`

---

## Benchmarks

Performance results from our evaluation on AWS c6id.8xlarge (32 cores, 64 GB RAM) instances using the TPC-C Benchmarks with different workload mixes:

### TPC-C Read Heavy Workload 

- This workload was ran on the `mehadi_TPCC branch`, the github workflow file for this is found at `.github/workflows/tpcc-fast-path-eval.yml`, the
evaluation script being ran can be found at `scripts/run_tpcc_fast_path_eval.sh` 
- Workload mix: 10% NewOrder, 10% Payment, 0% Delivery, 40% OrderStatus, 40% StockLevel (`MAKO_TPCC_WORKLOAD_MIX="10,10,0,40,40"`).
- Setup: single shard, replication enabled, 6 threads.

| Measurement | Baseline | Fast Path | Fast Path vs Baseline |
|---|---:|---:|---:|
| Aggregated Throughput (ops/s) | ~263,078 | ~1,109,300 | ~4.2× Increase |
| Latency (ms) | ~0.022238 | ~0.00484236 | ~4.6× Faster |
| Aggregated Abort Rate (aborts/s) | ~37.2275 | ~33,249.5 | ~900× Higher |

- For the summary results they can be found in `fast-path-results/Summary_Results/TPCC_ReadHeavy` and the full results are in `fast-path-results/Full_Result_Files/TPCC_ReadHeavy`

### TPC-C Read Only Workload   

- This workload was ran on the `mehadi_tpcc_read_only branch`, the github workflow file for this is found at `.github/workflows/tpcc-readonly-eval.yml`, the evaluation script being ran can be found at `scripts/run_tpcc_readonly_eval.sh` 
- Workload mix: 0% NewOrder, 0% Payment, 0% Delivery, 100% OrderStatus, 0% StockLevel (`MAKO_TPCC_WORKLOAD_MIX="0,0,0,100,0"`).
- Setup: single shard, replication enabled, 6 threads.

| Measurement | Baseline | Fast Path | Fast Path vs Baseline |
|---|---:|---:|---:|
| Aggregated Throughput (ops/s) | ~2,299,650 | ~2,613,880 | 1.137× Increase |
| Latency (ms) | 0.00207439 | 0.00174061 | 1.2× Faster |
| Aggregated Abort Rate (aborts/s) | 0 | 0 | Same |

- For the summary results they can be found in `fast-path-results/Summary_Results/TPCC_ReadOnly` and the full results are in `fast-path-results/Full_Result_Files/TPCC_ReadOnly` 

### TPC-C Write Heavy Workload  

- This workload was ran on the `mehadi_tpcc_write_heavy branch`, the github workflow file for this is found at `.github/workflows/tpcc-writeheavy-eval.yml`, the evaluation script being ran can be found at `scripts/run_tpcc_writeheavy_eval.sh` 
- Workload mix: 40% NewOrder, 40% Payment, 0% Delivery, 10% OrderStatus, 10% StockLevel (`MAKO_TPCC_WORKLOAD_MIX="40,40,0,10,10"`).
- Setup: single shard, replication enabled, 6 threads.

| Measurement | Baseline | Fast Path | Fast Path vs Baseline |
|---|---:|---:|---:|
| Aggregated Throughput (ops/s) | 329,154 | 442,401 | 1.34× Increase |
| Latency (ms) | 0.017645 | 0.0129555 | 1.36× Faster |
| Aggregated Abort Rate (aborts/s) | 92.6137 | 3,712.44 | 40× Higher |

- For the summary results they can be found in `fast-path-results/Summary_Results/TPCC_WriteHeavy` and the full results are in `fast-path-results/Full_Result_Files/TPCC_WriteHeavy`

### TPC-C MultiSharded Workload  

- This workload was ran on the `mehadi_tpcc_multi_shard branch`, the github workflow file for this is found at `.github/workflows/multishard-follower-read-eval.yml`, the evaluation script being ran can be found at `scripts/run_multishard_follower_read_eval.sh` 
- Workload mix: same as read-heavy (`MAKO_TPCC_WORKLOAD_MIX="10,10,0,40,40"`).
- Setup: 3 shards, 2 threads per shard; shards co-located on one machine to emulate a geo-replicated environment.

| Measurement | Baseline | Fast Path | Fast Path vs Baseline |
|---|---:|---:|---:|
| Aggregated Throughput (ops/s) | 114,374.2 | 244,040.7 | 2.1× Increase |
| Average Latency (ms) | ~0.026 | ~0.005 | 5.2× Faster |
| Aggregated Abort Rate (aborts/s) | 157.484 | ~2,676.243 | 17× Higher | 

- For the full results they can be found in `fast-path-results/Full_Result_Files/TPCC_MultiShard`

### Mako Fast Path Performance Summary & Advantages

**Single-shard benchmarks (1 shard, 6 threads):**

| Benchmark | Throughput | Latency | Abort Rate |
|---|---:|---:|---:|
| TPCC-Read Heavy | 4.2× Increase | 4.6× Faster | 900× Higher |
| TPCC-Read Only | 1.137× Increase | 1.2× Faster | Same |
| TPCC-Write Heavy | 1.34× Increase | 1.36× Faster | 40× Higher |

**Multi-shard benchmark (3 shards, 2 threads/shard):**

| Benchmark | Throughput | Latency | Abort Rate |
|---|---:|---:|---:|
| TPCC Multi-Sharded Read-Heavy | 2.1× Increase | 5.2× Faster | 17× Higher |

- TPCC-Read Heavy (single shard): 4.2× higher throughput, 4.6× lower latency, 900× higher abort rate
- TPCC-Read Only (single shard): 1.137× higher throughput, 1.2× lower latency, same abort rate
- TPCC-Write Heavy (single shard): 1.34× higher throughput, 1.36× lower latency, 40× higher abort rate
- TPCC Multi-Sharded Read-Heavy (3 shards): 2.1× higher throughput, 5.2× lower latency, 17× higher abort rate 

*Results from evaluation on AWS. Performance varies based on hardware, network topology, and workload characteristics.*

---

## Future Work

Several directions could further strengthen and extend this work:

1. **Adaptive Fast-Path Eligibility**
   Currently, read-only transactions either attempt the fast path or fall back to the baseline path based on fixed safety checks. Future work could dynamically enable or disable the fast path based on observed abort rates, workload mix, or shard skew.

2. **Improved Snapshot Selection**
   The current snapshot selection strategy is conservative to preserve safety. More aggressive snapshot selection or bounded staleness techniques (similar to CockroachDB’s follower reads) could reduce aborts while maintaining acceptable consistency guarantees.

3. **Fine-Grained Abort Attribution**
   Abort rates increased under certain workloads, but identifying *which* safety condition caused a fallback remains coarse-grained. Adding structured abort reasons and metrics would enable more targeted optimization.

4. **Expanded Benchmarking**
   While this project focuses on TPCC-based workloads, future evaluations could include YCSB-style microbenchmarks and latency-sensitive read workloads to better isolate read-only performance gains.

5. **Integration with Full MVCC Metadata**
   Deeper integration with Mako’s MVCC metadata (e.g., richer version tracking or commit watermarks) could enable safer fast-path execution with fewer conservative checks.

6. **Automated Experiment Orchestration**
   Experiment reproducibility was a challenge due to distributed execution and resource constraints. Future work could formalize experiment orchestration using parameterized workflows and result validation.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **Research Team**: Mako research and development team
- **Contributors**: All researchers and students who have contributed
- **Dependencies**: Built on excellent open-source projects including Janus, Masstree, RocksDB, eRPC, and many others

## References  
[Mako OSDI Paper](https://www.usenix.org/system/files/osdi25-shen-weihai.pdf)   
[CockroachDB Blogs](https://www.cockroachlabs.com/blog/)    
[Spanner](https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf)    
