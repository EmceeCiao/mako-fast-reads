# Fast Read‑Only Transaction Path & Follower Reads

This document defines the architecture and implementation plan for a **fast read‑only transaction path** in Mako, modeled on CockroachDB‑style follower reads but built on **Mako’s existing logical timestamps and watermark infrastructure** (no HLC required).

It is written as an “epic” for future work and explicitly notes where code is already partially implemented vs. what remains to be done. This is important because we cannot run full multi‑shard tests locally and will rely on CI and YCSB to validate correctness and performance.

---

## Relation to Original Project Proposal

The original project proposal described:

- A **read‑only transaction fast path** to bypass 2PC for read‑only workloads and reduce latency.  
- Integrating **MVCC** for versioned data access.  
- Using **Hybrid Logical Clocks (HLC)** to obtain globally monotonic timestamps without specialized hardware.  
- Adding **CockroachDB‑style follower reads** using shard‑level safe timestamps.  
- Future work: a centralized **“timestamp oracle”** to further reduce coordination.

This design keeps the spirit of that proposal but adapts it to Mako’s existing mechanisms:

- **Read‑only fast path:**  
  - We still implement a dedicated fast path that bypasses 2PC/Paxos for read‑only transactions and uses a per‑transaction snapshot timestamp, as originally envisioned.

- **MVCC:**  
  - Instead of building a new MVCC layer, we reuse Mako’s existing MVCC support: Masstree versions with `Node::timestamp` and `MultiVersionValue::mvGET`. This satisfies the “MVCC for versioned data access” goal with much less churn.

- **Global timestamps (HLC vs existing timestamps):**  
  - Conceptually, HLC in the proposal provided **globally monotonic logical timestamps**.  
  - In this design, we achieve a similar effect by reusing Mako’s single logical commit timestamp (`tid_unique_` / `Node::timestamp`) plus the **watermark exchange** mechanism to obtain a cluster‑wide “safe” timestamp.  
  - HLC remains a **future enhancement** behind the same abstraction: the doc keeps a future‑work section explaining how HLC could replace the current timestamp while preserving the API.

- **CockroachDB‑style follower reads:**  
  - We preserve the core idea of follower reads gated by **shard‑level safe timestamps** (closed timestamps).  
  - Safe timestamps come from `sync_util::sync_logger::single_watermark_` and `remoteExchangeWatermark`.  
  - Followers only serve reads when `T_safe_follower ≥ T_read`; otherwise, they abort and the txn can be retried on a leader (Option A).

- **“Timestamp oracle” idea:**  
  - The current design uses a **distributed watermark‑exchange mechanism** (via ERPC and `ShardClient`) instead of a single centralized oracle.  
  - A centralized timestamp oracle remains a natural **future direction**: it would be a specialized service that aggregates/advances safe timestamps for all shards and replicas, but is not required for the initial implementation described here.

---

## 1. Goals and Non‑Goals

### 1.1 Goals

- **Fast path for read‑only transactions**
  - Avoid 2PC/Paxos in the common case of read‑only workloads.
  - Keep serializable isolation and correctness guarantees.

- **Follower reads**
  - Allow followers to serve read‑only transactions when they are fresh enough.
  - Use a **closed timestamp / safe timestamp** concept, analogous to CockroachDB.

- **Minimal disruption to existing Mako design**
  - Reuse Mako’s **single commit timestamp per value** (the `Node::timestamp` field).
  - Reuse the **global watermark** infrastructure (`sync_util::sync_logger::single_watermark_`) as our safe/closed timestamp.
  - Avoid introducing a new timestamp system (e.g., HLC) for this project.

- **CI‑friendly design**
  - Changes should be localized and well‑documented to make debugging via CI feasible.
  - Explicitly identify invariants we depend on so we can add assertions/logging instead of heavy local testing.

### 1.2 Non‑Goals

- **Full “external consistency” tied to wall‑clock time.**
  - Cockroach uses HLC and clock synchronization to guarantee that a read “as of now()” sees all commits that completed before that wall‑clock instant.
  - In this project we **do not attempt to guarantee external consistency in wall‑clock terms**; instead we provide a strong **logical closed‑timestamp semantics**:
    - Reads at snapshot `T_read` see all commits with commit timestamp ≤ `T_read` that are safely replicated.

- **New MVCC data layout or per‑key version chains.**
  - Mako already uses timestamps as versions and maintains a workspace/multi‑version support (e.g., `MultiVersionValue::mvGET`). We reuse this instead of designing a new MVCC layer.

- **Implementing HLC.**
  - HLC is treated as **future work**. The design keeps a narrow abstraction layer so HLC could be dropped in later if desired, but we do not implement it now.

---

## 2. High‑Level Design Decisions

### 2.1 Timestamp Model

- **Commit timestamp source**
  - Keep using Mako’s existing **single logical timestamp**:
    - Values store `Node::timestamp` in Masstree nodes (see `mako::Node` layout).
    - `Transaction::tid_unique_` is a single logical timestamp, encoded as `timestamp*10 + epoch`.
  - We **do not change** this encoding in this epic.

- **Watermarks as safe / closed timestamps**
  - `sync_util::sync_logger::single_watermark_` is an `std::atomic<uint32_t>` storing `timestamp*10 + epoch` for the entire system.
  - A replica’s **safe read timestamp** is defined as:
    - `T_safe = sync_util::sync_logger::single_watermark_.load(...) / 10`.
  - For multi‑shard transactions, we compute a **global snapshot timestamp** by taking the **minimum** safe timestamp across all involved shards (using `remoteExchangeWatermark`).

- **No HLC for now**
  - We treat the current logical timestamp as our “transaction time” axis.
  - The design allows plugging in HLC later, but all logic in this epic is written assuming the existing `timestamp*10 + epoch` format.

### 2.2 Read‑Only Transaction Semantics

- A **read‑only fast‑path transaction**:
  - Is explicitly marked as read‑only by the caller (or by the scheduler) at **transaction start**.
  - Uses a single **snapshot timestamp `T_read`** for all its reads.
  - Commits via a specialized **fast commit path** that:
    - Validates that all read versions are still valid.
    - Checks that all returned data items are **safe**: `item_timestamp ≤ T_read` and `T_read ≤ T_safe` for all shards.
    - Skips locking, write installation, and Paxos replication.

- Follower read semantics (Option A – abort & retry):
  - Each replica maintains a local **safe timestamp** `T_safe_replica` via watermarks.
  - A follower **may serve** a read‑only txn at snapshot `T_read` **only if**:
    - `T_safe_follower ≥ T_read`.
  - If `T_safe_follower < T_read`, the follower:
    - **Aborts** the transaction (using `Sto::abort_without_throw()`).
    - The client or higher layer is responsible for retrying, potentially on a leader.
  - This matches the Cockroach “closed timestamp” pattern logically, with **Option A** implementation: redirect via abort+retry, not via RPC forwarding.

### 2.3 “Cockroach‑like” vs. Exact Cockroach Behavior

- We provide **Cockroach‑style follower reads in terms of closed timestamps** (logical time), not in terms of physical wall‑clock time.
- The key guarantee we aim for:
  - Transactions see a **consistent snapshot** at timestamp `T_read`.
  - No commit with timestamp ≤ `T_read` that is fully replicated can be missing from that snapshot.
  - Followers only serve reads when their `T_safe_follower` is ≥ `T_read`.
- This is sufficient for the project’s goal of “CockroachDB‑style follower reads” from a concurrency‑control perspective, but we do **not** claim full external consistency relative to NTP.

---

## 3. Existing Building Blocks and Their Status

This section documents the pieces that already exist in the repository and whether they are **fully wired**, **partially wired**, or **unused** today. The epic phases later will explicitly reference these.

> Caveat: The code base has evolved over time, and some features are partially implemented but not enabled. We call these out explicitly so reviewers know where the risk lies.

### 3.1 Logical Commit Timestamps

- **Where:** `src/mako/benchmarks/sto/Transaction.hh` and related Masstree code.
- **Mechanism:**
  - `Transaction::tid_unique_` serves as a single timestamp, encoded as `timestamp*10 + epoch`.
  - Masstree nodes (`mako::Node`) carry a `timestamp` field written on commit.
  - The Masstree value layout includes both the user value and the `Node` header, so a transaction can recover `item_timestamp` from `versioned_str_struct` data.
- **Status:** **In use**, and relied upon by existing replication and persistence logic.

### 3.2 Watermarks and Safe Timestamps

- **Where:** `src/mako/benchmarks/sto/sync_util.hh` (`sync_util::sync_logger`), `src/mako/lib/shardClient.{h,cc}`, `src/mako/lib/common.h`.
- **Mechanism:**
  - `single_watermark_` stores a **global watermark**: `timestamp*10 + epoch` for the system.
  - `computeLocal()` and `advancer()` compute a local minimum over replication and disk timestamps and push that into `single_watermark_`.
  - `client_watermark_exchange()` periodically exchanges watermarks via ERPC (`ShardClient::remoteExchangeWatermark`), using `watermarkReqType` RPC codes.
  - `retrieveW()` returns the current watermark.
- **Status:**
  - Watermarks are **actively used** for safety checks and remote validation.
  - The codepath for periodic watermark exchange and updating `single_watermark_` is implemented, but the exact deployment / configuration needs to be validated during CI.

### 3.3 Read‑Only Fast Path in Transaction

- **Where:** `src/mako/benchmarks/sto/Transaction.hh` and `src/mako/benchmarks/sto/Transaction.cc`.
- **Key pieces:**
  - Flags and state:
    - `mutable bool is_read_only_fast_path_{false};`
    - `mutable uint32_t read_timestamp_{0};`
  - API:
    - `void set_read_only_fast_path(bool value = true);`
    - `bool is_read_only_fast_path() const;`
    - `void acquireReadTimestamp();`
    - `uint32_t getMinSafeTimestamp() const;`
    - `bool try_commit_read_only();`
  - Commit routing:
    - `Transaction::commit()` dispatches to `try_commit_read_only()` if `is_read_only_fast_path_ && !has_any_writes()`.
    - `Sto::try_commit()` does the same at the static helper level.
  - Safe timestamp computation:
    - `getMinSafeTimestamp()` uses `sync_util::sync_logger::retrieveW()` and `ShardClient::remoteExchangeWatermark()` and returns the **minimum** safe timestamp across involved shards (after dividing by 10).
  - Fast‑path commit:
    - `Transaction::try_commit_read_only()`:
      - Computes `read_timestamp_ = getMinSafeTimestamp()`.
      - Validates that for each read item, `item_timestamp ≤ read_timestamp_` (by examining `mako::Node::timestamp` through the `versioned_str_struct` layout).
      - Validates read versions (`owner()->check()`).
      - Optionally validates remote reads via `remoteValidate()` and updates the watermark.
      - Skips locking, write installation, and Paxos.
- **Status:**
  - Implementation is present and **logically complete** for the leader‑only fast path.
  - **Not yet used by any callers**: no code calls `set_read_only_fast_path()`, so the fast path is effectively disabled by default.
  - `acquireReadTimestamp()` is implemented but **not called**, which matters for follower reads (see below).

### 3.4 Follower Read Helpers

- **Where:** `src/mako/benchmarks/sto/sync_util.hh` and `src/mako/benchmarks/sto/MassTrans.hh`.
- **Helpers:**
  - In `sync_util::sync_logger`:
    - `static bool can_serve_follower_read(uint32_t requested_ts);`
    - `static uint32_t getFollowerSafeTimestamp();`
    - `static bool should_redirect_to_leader(uint32_t requested_ts);`
    - `static bool isLeader();`
  - In `MassTrans` (Masstree wrapper):
    - `template <typename ValType> bool transGet(Str key, ValType& retval, ...)` performs a follower‑read check:
      - If `TThread::txn && TThread::txn->is_read_only_fast_path()` and `read_timestamp_ > 0`:
        - Call `sync_util::sync_logger::should_redirect_to_leader(read_ts)`.
        - If true, call `Sto::abort_without_throw()` and set `TThread::transget_without_throw = true`.
    - Helper:
      - `bool canServeFollowerRead(uint32_t read_timestamp) const` simply wraps `can_serve_follower_read`.
- **Status:**
  - Code is **present but partially wired**:
    - Follower gating in `transGet()` depends on `read_timestamp_ > 0`.
    - Today, `read_timestamp_` is typically only set inside `try_commit_read_only()` (at commit time), not before reads.
    - The `acquireReadTimestamp()` method exists but is never called prior to reads.
  - As a result, follower read checks are effectively **no‑ops in the current code** because `read_timestamp_` remains 0 during the read phase.

### 3.5 Read‑Only Classification in Higher Layers

- **Where:**
  - Generic transaction flags: `src/mako/txn.h` (`TXN_FLAG_READ_ONLY`) used by the abstract DB layer and some benchmarks.
  - Mako/sto path: `src/mako/benchmarks/sto/Transaction.hh`, `Sto` helpers (`Sto::start_transaction`, `Sto::start_read_only_transaction`, `Sto::try_commit`).
- **Status:**
  - The generic **read‑only flagging mechanism** (`TXN_FLAG_READ_ONLY`) exists for higher‑level code, but the sto fast‑path flag (`is_read_only_fast_path_`) is now managed primarily inside the sto implementation:
    - `Sto::start_transaction()` starts a generic transaction.
    - `Sto::start_read_only_transaction()` starts a transaction with `is_read_only_fast_path_` set and an early snapshot, useful for examples/tests.
    - Earlier iterations experimented with **automatic promotion at commit time** (“if no writes, treat as fast‑path read‑only”), but this proved fragile: it interacted badly with fast‑path fallbacks (leading to recursion/looping in CI) and made serializability reasoning harder. The epic now treats the fast path as an **explicit mode chosen at transaction start**, not something inferred or promoted mid‑transaction or mid‑commit.
  - **Caveat:** The fast read‑only path depends on the closed‑timestamp / replication machinery. When `BenchmarkConfig::getInstance().getIsReplicated()` is false (single‑replica runs), fast‑path helpers are effectively disabled so the system falls back to the baseline Mako behavior. Any performance comparisons should note that follower‑read gains only apply to replicated deployments.

---

## 4. Architectural End State (What We Want)

This section summarizes the desired end state, independent of how we get there. The epic phases in §5 define the path to reach it.

### 4.1 Snapshot and Safe Timestamp

- At the **start** of a read‑only fast‑path transaction:
  - The system **selects a snapshot timestamp** `T_read`:
    - `T_read = Transaction::getMinSafeTimestamp()`.
    - This uses the local watermark and any remote watermarks for shards in the read set.
  - `read_timestamp_` is set once and used for the entire lifetime of the transaction.

- Invariants:
  - `T_read` is **at or before** the current safe watermark on each involved shard when selected.
  - Watermarks only advance once commits are fully replicated (and, if enabled, persisted).

### 4.2 Read Path (Leader & Follower)

- **Leader reads:**
  - On a leader, `can_serve_follower_read(T_read)` always returns true.
  - Reads are served from local Masstree at snapshot `T_read` and validated by the existing `owner()->check()` path and commit‑time checks.

- **Follower reads:**
  - On a follower, before serving `transGet()` for a txn marked `is_read_only_fast_path_`:
    - Compute `T_safe_follower = sync_util::sync_logger::single_watermark_ / 10`.
    - If `T_safe_follower ≥ T_read`, serve the read locally.
    - Otherwise, **abort the transaction** (`Sto::abort_without_throw()`), and higher layers may retry on a leader.

- This yields a **closed‑timestamp semantics**:
  - Followers only serve reads when they are known to be at least as up‑to‑date as the chosen snapshot `T_read`.

### 4.3 Commit Path

- For read‑only fast‑path txns:
  - `Transaction::commit()` routes to `try_commit_read_only()`.
  - `try_commit_read_only()`:
    - Uses the **already chosen** `T_read` if it exists; otherwise it computes one using `getMinSafeTimestamp()` as today.
    - Verifies durability: for each read item, `item_timestamp ≤ T_read`.
    - Verifies opacity / serializability via `owner()->check()` and remote validation when applicable.
    - Performs **no locking, write installation, or replication**.

- For non‑read‑only or write transactions:
  - `commit()` falls back to the normal slow path (`try_commit()`), unchanged.

### 4.4 User‑Facing API

- Add an explicit API for read‑only fast path, e.g.:

```cpp
Sto::start_read_only_transaction();  // New helper
TThread::txn->set_read_only_fast_path(true);
TThread::txn->acquireReadTimestamp();  // snapshot chosen up front
```

- Benchmarks and YCSB integrations will be updated to use this API for pure read workloads.

### 4.5 Behavior on Stale Followers (Option A)

- If a follower is too stale (`T_safe_follower < T_read`):
  - The local read aborts the transaction via `Sto::abort_without_throw()`.
  - The client or scheduler is responsible for **retrying** the transaction.
  - A typical retry policy:
    - First retry on a leader to avoid repeated aborts.
    - Optionally track follower lag metrics to avoid scheduling fresh read‑only txns on badly lagging followers.

---

## 5. Epic Phases

Below is the concrete multi‑phase plan, annotated with **current status** so it’s clear what remains to be done. Phases should be implemented in order where dependencies exist, but some can be parallelized.

### Phase 0 – Baseline, Invariants, and Instrumentation

- **Objective:** Make explicit the invariants we rely on and add basic logging to help debug via CI.

- Tasks:
  - Document invariants in this file (done in §4) and in short comments in the code paths that enforce them (`Transaction::getMinSafeTimestamp`, `try_commit_read_only`, `sync_util::sync_logger` watermark logic).
  - Add low‑overhead logging/counters for:
    - Watermark values and their progression.
    - When `try_commit_read_only()` is used vs. normal `try_commit()`.
    - Follower read aborts due to staleness.

- **Status:** Invariants partially documented here; logging/counters to be added as needed during implementation.

---

### Phase 1 – Read‑Only Classification & API Wiring

- **Objective:** Ensure the system can recognize read‑only transactions **early** and route them down the fast path.

- Tasks:
  - Introduce a helper `Sto::start_read_only_transaction()` that:
    - Ensures a `Transaction` exists.
    - Calls `Transaction::start()`.
    - Calls `set_read_only_fast_path(true)`.
    - Calls `acquireReadTimestamp()` so `read_timestamp_` is set before any reads.
  - Make this helper available to benchmarks/examples that want to **explicitly** opt into the fast path. We deliberately do **not** rely on automatic “no‑writes ⇒ fast‑path” promotion at commit, because that complicates correctness and was observed to cause recursion/loop issues when the fast path must fall back.
  - Ensure `Transaction::start()` resets `is_read_only_fast_path_` and `read_timestamp_` (already true today).

- **Current code status:**
  - `set_read_only_fast_path()` and `acquireReadTimestamp()` exist and are used by `Sto::start_read_only_transaction()`; higher‑level code may call this helper explicitly, but the fast path no longer depends on external flags.
  - `Transaction::start()` does reset the flags (lines ~490–498 in `Transaction.hh`).

---

### Phase 2 – Leader‑Only Fast Read‑Only Commit Path

- **Objective:** Stabilize and rely on the fast commit logic for leader‑only read‑only txns.

- Tasks:
  - Review and, if necessary, refine `Transaction::getMinSafeTimestamp()` and `try_commit_read_only()`:
    - Ensure `getMinSafeTimestamp()` correctly uses `retrieveW()` and remote exchange to compute the minimum safe timestamp across shards.
    - Confirm the logic for extracting `item_timestamp` from Masstree values is correct and robust.
  - Add assertions that:
    - `read_timestamp_ > 0` when entering `try_commit_read_only()` for fast‑path txns.
    - `item_timestamp` is never greater than `read_timestamp_` for values returned to the user.
  - Gradually enable the fast path for leader‑only read‑only benchmarks (in CI) before turning on follower reads.

- **Current code status:**
  - `getMinSafeTimestamp()` and `try_commit_read_only()` are **implemented** and integrated into `Transaction::commit()` and `Sto::try_commit()`.
  - They are **not used in practice** because read‑only fast path flagging is not yet wired from callers.

---

### Phase 3 – Follower Read Safety (Option A)

- **Objective:** Make follower reads safe by gating them on the closed timestamp and aborting when followers are stale.

- Tasks:
  - Wire `acquireReadTimestamp()` into the read‑only transaction start path so `read_timestamp_` is set before `MassTrans::transGet()` runs.
  - Confirm and, if needed, adjust `sync_util::sync_logger::can_serve_follower_read()` and `should_redirect_to_leader()`:
    - Ensure they interpret `single_watermark_` as `timestamp*10 + epoch` and divide by 10 consistently.
  - Confirm `MassTrans::transGet()` behavior:
    - On a follower, for read‑only fast‑path txns with `read_timestamp_ > 0`, call `should_redirect_to_leader(read_timestamp_)`.
    - If true, call `Sto::abort_without_throw()` and set `TThread::transget_without_throw` so callers can interpret the result as “aborted due to follower staleness”.
  - Document expected retry behavior at the benchmark/client layer.

- **Current code status:**
  - Follower helpers in `sync_util.hh` and the abort path in `MassTrans.hh::transGet()` are **in place** but **ineffective** because `read_timestamp_` is typically 0 during reads.
  - This phase will make them actually functional by ensuring snapshot acquisition happens before the first read.

---

### Phase 4 – Integration with Scheduler & Benchmarks

- **Objective:** Make it easy for benchmarks and higher‑level components to benefit from the fast read‑only path and follower reads, while keeping the commit logic simple and avoiding mid‑transaction promotion complexity.

- Tasks:
  - Ensure existing benchmarks (e.g., TPCC, YCSB) can run unchanged and still benefit from the fast path when they **explicitly** opt in (e.g., via `Sto::start_read_only_transaction()` for dedicated experiments).
  - Keep sto’s fast‑path behavior independent of `TXN_FLAG_READ_ONLY` for correctness:
    - Higher‑level flags and hints may still be used for benchmark‑level reporting or tuning.
    - The core sto implementation will treat “fast‑path read‑only” as an explicit mode chosen at transaction start, not something inferred mid‑transaction.
  - Maintain a simple, well‑scoped commit path:
    - `try_commit_read_only()` is only used for transactions that were started in read‑only fast‑path mode.
    - Normal `try_commit()` handles all other cases (including transactions that happen to be read‑only but were not explicitly marked).

- **Current code status:**
  - `Sto::start_read_only_transaction()` is available and wires `set_read_only_fast_path(true)` + `acquireReadTimestamp()`.
  - Benchmarks can opt in to the fast path for specific read‑only workloads; they are not required to do so for correctness.
  - YCSB/TPCC wiring for follower‑read abort handling (`TThread::transget_without_throw`) is in place; further tuning and evaluation is part of Phase 6.

---

### Phase 5 – Snapshot‑Based Follower Reads (Silo/Mako‑Style)

- **Objective:** Explore a safer, snapshot‑based follower‑read design inspired by Mako’s paper and Silo’s high‑level approach, instead of automatic mid‑transaction promotion. The aim is to let read‑only transactions execute directly on followers by reading from a **periodic snapshot** that is known to be safe w.r.t. replication.

- **Current code status (partial):**
  - Read‑only fast‑path transactions now use `mvGET_snapshot()` in `MultiVersionValue` to pick the newest version whose logical timestamp ≤ `read_timestamp_`. This ensures fast‑path reads observe a consistent snapshot (`T_read`) while follower gating (`should_redirect_to_leader`) still enforces staleness checks.
  - Range scans (`transQuery` / `transRQuery`) use the same snapshot helper.
  - A dedicated checkpointing / promotion thread and the full “2‑version” steady‑state described below remain future work.

- Motivation (from Mako’s paper and Silo [95]):
  - The current Mako implementation does **not** let read‑only transactions execute directly on followers.
  - Mako is compatible with an optimization where:
    - A dedicated checkpointing thread periodically snapshots the database.
    - Read‑only transactions read from this snapshot instead of the live, speculative state.
    - Each key maintains two “special” versions:
      - One reflecting the latest value.
      - One storing the most recent value up to the last all‑agreed vector watermark (safe for read‑only txns).
  - This is conceptually similar to CockroachDB’s follower reads, which allow followers to serve reads at or before a **closed timestamp** that is known to be safe across replicas.

- High‑level design for Mako‑style snapshot follower reads:
  - **Checkpointing thread:**
    - A background thread on each shard periodically:
      - Captures a consistent snapshot of the database at a chosen timestamp `T_snap`.
      - Ensures `T_snap` is ≤ the last all‑agreed (vector or single) watermark so that the snapshot only contains fully replicated data.
  - **Dual versions per key (2‑version MVCC, no GC needed):**
    - For each logical key, maintain at most two logical versions:
      - `latest_value`, `latest_ts`: the newest value (possibly speculative, ahead of the watermark).
      - `stable_value`, `stable_ts`: the last value known to be fully replicated and ≤ the last all‑agreed watermark (safe for read‑only txns).
    - On a write, update only `latest_value` / `latest_ts` in place.
    - When the watermark advances so that `latest_ts` becomes safe with respect to replication, **promote**:
      ```cpp
      stable_value = latest_value;
      stable_ts    = latest_ts;
      ```
    - There is never an unbounded chain of versions per key; the “GC” of older snapshot versions is implicit in overwriting `stable_value`. Full MVCC garbage collection is not required for the snapshot layer.
    - Snapshot storage can reuse existing per‑key versioned layout (Masstree + `Node::timestamp` + `MultiVersionValue`) without adding a separate version chain.

  - **Why this 2‑version MVCC needs no GC:**
    - In full MVCC, each update creates a new version (`v@t1`, `v@t2`, `v@t3`, …) and a garbage collector must eventually reclaim versions older than the oldest active snapshot.
    - Here, each key stores **at most two versions**:
      - A latest version (`latest_value@latest_ts`) that tracks the current state.
      - A stable version (`stable_value@stable_ts`) that tracks the last state proven safe w.r.t. the all‑agreed watermark.
    - When the watermark moves forward and `latest_ts` becomes safe, we simply **overwrite** the stable slot:
      - No third version is ever created; the old stable version is discarded in place.
      - Memory usage is therefore bounded by 2× the value size per key, and no background GC walker is needed at the MVCC layer.
  - **Read‑only path:**
    - Pure read‑only transactions that tolerate bounded staleness:
      - Are routed to read from the “snapshot” versions at followers (or leaders).
      - Do not participate in speculative 2PC/Paxos; they see a consistent view frozen at `T_snap`.
    - The snapshot timestamp `T_snap` plays a role analogous to CockroachDB’s closed timestamp:
      - Followers can safely serve any read at timestamps ≤ `T_snap`.
      - New snapshots advance over time as watermarks progress.
  - **Interaction with existing watermarks:**
    - The snapshot timestamp `T_snap` should be derived from the all‑agreed vector/single watermark:
      - For a multi‑shard configuration, take the min across shards (as in `getMinSafeTimestamp()`).
      - Followers only advertise `T_snap` values that are ≤ their local and remote safe timestamps.
  - **Consistency and isolation:**
    - Snapshot reads are strictly read‑only and see a consistent prefix of the global history.
    - They do not affect speculative writes or commit ordering.
    - From the concurrency‑control perspective, a snapshot read‑only transaction is **serializable at its snapshot timestamp** (`stable_ts` / `T_snap`):
      - Its outcome is equivalent to running atomically at logical time `T_snap` in the history.
      - It may be **stale** relative to real time (“not linearizable to now”), but still respects serializability when combined with read‑write transactions that use Mako’s existing speculative 2PC protocol.
    - The main correctness questions become:
      - How to publish new snapshots (`T_snap`) without violating consistency.
      - How to manage memory for the dual‑version representation (bounded 2‑version per key) without needing a full MVCC garbage collector.
  - **Snapshot semantics and serializability level:**
    - Base Mako already targets **serializable** transactions (with strict guarantees for read‑write txns via speculative 2PC and watermark‑based safety).
    - Snapshot‑based read‑only txns that read from `stable_value@stable_ts`:
      - Observe a consistent snapshot of the database at logical time `stable_ts` (or `T_snap`), derived from the all‑agreed watermark.
      - Are **serializable** at that snapshot time: their outcome is equivalent to executing atomically at `stable_ts` in the history, even though they may be stale relative to “now”.
      - Do not necessarily provide “read‑your‑writes” or wall‑clock external consistency relative to concurrent read‑write txns (they are snapshot‑isolated in time, not necessarily linearizable to real time).
    - Follower snapshot reads should align with CockroachDB’s closed‑timestamp semantics:
      - Followers only serve reads at timestamps ≤ a closed timestamp (`T_snap`/`stable_ts`) that is known to be safe cluster‑wide.
      - Combined with Mako’s existing serializable commit protocol, this provides a high‑level serializable read path where:
        - Read‑write txns remain strictly serializable.
        - Snapshot read‑only txns are serializable at their chosen (possibly stale) snapshot timestamp.

- Tasks:
  - Prototype a **checkpointing thread**:
    - Decide on a snapshot interval and the representation of `T_snap`.
    - Integrate with existing watermark logic so `T_snap` ≤ last all‑agreed watermark.
  - Decide how to represent **snapshot versions**:
    - Option A: encode snapshot values as part of the existing version chains (using `Node::timestamp` to identify which version belongs to which snapshot).
    - Option B: maintain an auxiliary snapshot index that maps keys to snapshot values and timestamps.
  - Design the **read‑only API**:
    - A “snapshot read‑only mode” that routes transactions to snapshot storage and does **not** interfere with the main transactional path.
    - Decide whether snapshot reads can be initiated at followers only, or at both leaders and followers, and how clients choose between “fresh” (leader) vs. “snapshot” (follower) reads.
  - Align semantics with CockroachDB‑style follower reads:
    - Document how `T_snap` relates to the closed timestamp concept:
      - `T_snap` ≤ closed timestamp at all replicas serving snapshot reads.
    - Ensure snapshot reads never see values beyond `T_snap`, even if local replication progress is ahead.
  - Treat this as future work:
    - This snapshot‑based design is intentionally more complex than the Phase 0–4 work and is not implemented in this repo yet.
    - It should be evaluated carefully against Mako’s existing MVCC, watermark, and speculative 2PC machinery before implementation.

- **Current code status:**
  - The current repo implements a leader‑only fast read‑only commit path plus follower safety checks based on watermarks.
  - We **do not** implement automatic mid‑transaction promotion to the fast path, due to complexity and potential serializability pitfalls.
  - Snapshot‑based follower reads are a **design direction** informed by Mako’s paper and Silo/CockroachDB ideas, not implemented code.

---

### Phase 6 – CI/CD Validation and YCSB Evaluation

- **Objective:** Validate correctness and performance using CI pipelines and YCSB, since local multi‑shard testing is not feasible.

- Tasks:
  - Extend CI to run:
    - 1‑shard and 2‑shard replication tests with read‑only fast path enabled.
    - Scenarios where followers lag (e.g., introducing artificial delays) to exercise abort‑and‑retry behavior.
  - Run YCSB benchmarks:
    - Baseline: current Mako behavior without fast path/follower reads.
    - Experimental: with fast read‑only path and follower reads enabled.
    - Compare throughput and latency, focusing on read‑heavy workloads.
  - Monitor and log:
    - Number of fast‑path commits vs. normal commits.
    - Number of follower‑staleness aborts.

- **Current code status:**
  - CI scripts exist for replication tests (`ci/ci.sh`, various `test_*replication*.sh`).
  - A dedicated **fast read‑only CI microbenchmark** is wired into GitHub Actions:
    - Binary: `examples/fastPathReadOnly.cc`.
    - Shell driver: `examples/test_fast_path_readonly.sh`.
    - CI entry: `./ci/ci.sh fastPathReadOnly` (used twice with different modes).
  - Exact configuration of the fast read‑only CI microbenchmark:
    - Topology:
      - **1 logical shard** (`nshards = 1`, `shardIdx = 0`).
      - **Replication enabled** (`is_replicated = 1`), using the standard Paxos configs:
        - Shard config: `src/mako/config/local-shards1-warehouses4.yml`.
        - Paxos configs: `config/1leader_2followers/paxos4_shardidx0.yml` and `config/occ_paxos.yml`.
      - Cluster roles (processes started in CI):
        - `localhost` (leader, `BenchmarkConfig::getLeaderConfig() == true`).
        - `p1` and `p2` (followers).
        - `learner` (learner replica).
    - Threads:
      - `nthreads = 4` on each process (`BenchmarkConfig::setNthreads(4)`).
    - Workload (leader only):
      - Priming phase:
        - Opens the `customer_0` Masstree table via `mbta_sharded_ordered_index`.
        - Inserts **16 keys** `fast_ro_key_0_i` (for `i = 0..15`) with values `fast_ro_value_0_i` using normal replicated transactions (no fast‑path flags).
      - Read‑only benchmark phase:
        - Runs `iterations = 100` transactions in a tight loop.
        - Each transaction:
          - Calls `db->new_txn(0, arena, buf)` via `mbta_wrapper`, which internally uses `Sto::start_transaction()`.
          - If fast‑path mode is enabled (see below), obtains the sto transaction via `Sto::transaction()` and calls:
            - `set_read_only_fast_path(true);`
            - `acquireReadTimestamp();`
          - Issues a single `table->get(txn, key, value)` on one of the 16 primed keys (index `i % 16`).
          - Calls `db->commit_txn(txn)`, retrying on aborts.
        - Followers (`p1`, `p2`, `learner`) run the same binary but only sleep while hosting Paxos services; they do not issue user transactions.
      - Modes (controlled by `FAST_RO_MODE` environment variable in CI):
        - `FAST_RO_MODE=baseline`:
          - The benchmark **does not** call `set_read_only_fast_path()` / `acquireReadTimestamp()`.
          - All transactions use the **baseline** sto commit path (`try_commit()` with 2PC/Paxos).
        - `FAST_RO_MODE=fast`:
          - The benchmark sets `is_read_only_fast_path_` and takes the **read‑only fast path** (`try_commit_read_only()`), including snapshot selection via `read_timestamp_` and follower‑read gating in `MassTrans::transGet()`.
      - Metrics:
        - The leader logs a single structured line summarizing the microbenchmark:
          ```text
          FAST_RO_STATS mode=<baseline|fast> iterations=<N> commits=<N>
                         duration_ms=<wall time of 100 txns>
                         throughput_ops_per_sec=<commits / (duration_ms/1000)>
          ```
        - The CI script `examples/test_fast_path_readonly.sh` greps this line from the leader’s log (`fastpath-shard0-localhost.log`) and echoes it into the GitHub Actions log for easy comparison.
      - CI wiring:
        - Workflow: `.github/workflows/ci.yml`.
        - Two steps run the same test with different modes:
          - `FAST_RO_MODE=baseline ./ci/ci.sh fastPathReadOnly`
          - `FAST_RO_MODE=fast ./ci/ci.sh fastPathReadOnly`
        - A final aggregator step fails the job if either mode fails.
  - YCSB evaluation is planned but not yet wired up in this repo.

---

### Phase 7 – Future Work: HLC and External Consistency

This phase is **explicitly not part of the current implementation**, but we document it to show how the design can be extended.

- **HLC integration (future)**
  - Implement a Hybrid Logical Clock (HLC) that:
    - Combines physical NTP time with a logical counter.
    - Provides monotonic timestamps across nodes.
  - Replace the underlying commit timestamp generation with HLC, while keeping the same high‑level API:
    - `Transaction::getMinSafeTimestamp()` and watermark logic would now work over HLC‑encoded values.
  - This would allow us to reason about follower reads not just in logical commit order, but also in terms of **real‑time staleness bounds**.

- **Centralized timestamp oracle (future)**
  - Build on top of HLC or the existing logical timestamp by introducing a **centralized or logically centralized “timestamp oracle”** component:
    - Responsible for issuing monotonic read/commit timestamps and tracking safe/closed timestamps for follower reads.
    - Could further reduce coordination by letting shards consult the oracle instead of performing pairwise watermark exchange.
  - This would subsume the current ERPC watermark‑exchange logic but is deliberately deferred until the simpler, decentralized design is validated.

- **External consistency**
  - With HLC and proper integration, we could target an external consistency property closer to CockroachDB’s: reads “as of now()” see all commits that completed before `now()` given bounded clock skew.

- **Why we defer this**
  - Introducing HLC touches core timestamp generation and would significantly increase the risk surface.
  - Given we cannot run heavy tests locally, we prioritize a simpler logical‑timestamp design that is easier to validate via CI and YCSB.

---

## 7. Summary

- We will implement a **fast read‑only transaction path** and **follower reads** in Mako by:
  - Reusing Mako’s existing **logical commit timestamps** and **watermarks** as a closed‑timestamp system.
  - Explicitly marking read‑only transactions and choosing a **snapshot timestamp `T_read`** at transaction start.
  - Using **fast commit** logic that validates read versions and durability without going through 2PC/Paxos.
  - Allowing followers to serve reads only when their local safe timestamp is ≥ `T_read`, otherwise aborting and retrying (Option A).

- The current code already contains many of the required building blocks (watermarks, fast‑path commit, follower helpers), but they are **not yet fully wired**. This epic provides a roadmap to safely integrate and validate them under realistic workloads and CI constraints, without introducing the complexity of HLC in the first iteration.
