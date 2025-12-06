# Dual-Version MVCC Epic for Fast Read‑Only Path

This document defines an **implementation epic** for bringing Mako’s fast read‑only path fully in line with the “2‑version MVCC” design sketched in the OSDI paper and in `doc/read_only_fast_path.md` (Phase 5).

The goal is to evolve the existing single‑chain MVCC (Masstree + `MultiVersionValue`) into a design where each logical key exposes two logical versions:

- A **latest** version: newest committed/speculative value.
- A **stable** version: newest value whose timestamp is ≤ the closed/replicated watermark (safe for follower/snapshot reads).

Snapshot/follower reads should consume only the stable version (or a structure derived from it), without depending on historical chains that GC may reclaim.

This epic is future work; the current repo does **not** implement the dual‑slot invariant yet (see §2).

---

## 1. Current State and Gaps

### 1.1 Current MVCC Behavior

- **Per‑key representation**
  - `MultiVersionValue::mvInstall` (`src/mako/benchmarks/sto/multiversion.hh`) maintains a **single chain** of versions per key:
    - The head is the latest value (with `Node::timestamp`).
    - Older versions are linked via `header->data` and `header->data_size`.
  - There is no explicit second “stable” slot (`stable_value`, `stable_ts`) separate from the chain.

- **Snapshot reads**
  - `MultiVersionValue::mvGET_snapshot(std::string& val, char* base, uint32_t snapshot_ts)` walks the chain backwards and returns the newest version with `timestamp <= snapshot_ts`.
  - This is used by:
    - `MassTrans::transGet` for read‑only fast‑path transactions.
    - Range queries (`transQuery` / `transRQuery`) when `TThread::txn->is_read_only_fast_path()` and multiversion is enabled.
  - Snapshot reads currently **depend on the historical chain** being present.

- **GC and watermarks**
  - `MultiVersionValue::lazyReclaim` consults `sync_util::sync_logger::retrieveShardW_relaxed() / 10` as a watermark and:
    - Walks the chain to find versions older than the watermark.
    - Truncates and frees old nodes, shrinking the chain.
  - `sync_util::sync_logger` maintains:
    - `single_watermark_` (closed timestamp, encoded as `timestamp*10 + epoch`).
    - `hist_timestamp` (per‑term stable timestamps).
  - Watermarks are used for:
    - Safety checks (`safety_check`).
    - GC thresholds (`lazyReclaim`).
  - Watermarks are **not** used to maintain a separate stable slot per key.

### 1.2 Gaps vs. 2‑Version MVCC Design

The Phase‑5 design in `doc/read_only_fast_path.md` calls for:

1. **Two logical versions per key**:
   - Latest (possibly ahead of watermark).
   - Stable (≤ watermark, safe).
2. **Promotion on watermark advance**:
   - When watermark passes `latest_ts`, promote `latest_value` → `stable_value`.
3. **Snapshot/follower reads over stable slot**:
   - Snapshot reads *must not* depend on historical chains that GC can reclaim.

Today:

- There is only one physical representation per key (the chain).
- Promotion is not implemented; GC runs purely for reclamation.
- Snapshot reads walk the chain and can fail once `lazyReclaim` trims versions.

The epic below closes this gap.

---

## 2. Goals and Non‑Goals

### 2.1 Goals

- **Two logical versions per key**
  - Maintain `latest` and `stable` notions for every key that has at least one committed version.

- **Safe promotion**
  - Ensure writes update `latest` immediately.
  - Ensure watermark advancement promotes `latest` into `stable` when safe.

- **Snapshot/follower reads over stable**
  - Follower reads and fast read‑only snapshot reads consult only stable data (or structures derived from it).
  - GC must never remove the currently chosen stable value.

- **Minimal disruption**
  - Keep Masstree and `versioned_value` layout intact if possible.
  - Centralize 2‑slot logic in `MultiVersionValue` + helpers, not across random call sites.

### 2.2 Non‑Goals

- No new multi‑key MVCC protocol (we continue to use the existing speculative 2PC + watermarks).
- No HLC/timestamp oracle changes in this epic (those remain in `doc/read_only_fast_path.md` Phase 7).
- No new CI microbenchmarks; verification is via assertions, instrumentation, and existing tests.

---

## 3. Phase 0 – Clarify Invariants & Add Instrumentation

**Objective:** Make the 2‑slot invariants explicit and add cheap instrumentation.

**Tasks:**

1. **Document invariants in code**
   - In `multiversion.hh`, above `MultiVersionValue`:
     - Describe desired semantics:
       - Latest = head of chain.
       - Stable = newest version ≤ watermark.
       - GC must never remove stable.
   - In `sync_util.hh`, near `single_watermark_`:
     - Note that the closed watermark defines the promotion boundary for stable values.

2. **Add counters / debug hooks**
   - In `MultiVersionValue`, add internal (static) counters:
     - Number of promotions (latest→stable).
     - Number of snapshot reads that could not find a suitable stable version.
   - Provide a simple debug helper to print these stats (for use in ad‑hoc tests, not CI).

**Verification:**

- Code review: invariants are documented where the machinery lives.
- Debug builds can optionally dump the counters at shutdown.

---

## 4. Phase 1 – Represent Stable Alongside Latest

**Objective:** Introduce a clear abstraction for “latest” and “stable” without changing external APIs.

**Tasks:**

1. **Add a logical view helper**

   In `MultiVersionValue`, add a struct and helper:

   ```cpp
   struct LogicalVersions {
       std::string latest_value;
       uint32_t latest_ts{0};
       std::string stable_value;
       uint32_t stable_ts{0};
       bool has_stable{false};
   };

   static LogicalVersions inspect_versions(std::string val,
                                           uint32_t watermark_ts);
   ```

   - `inspect_versions` should:
     - Treat `val` as the current head.
     - Decode `latest_value` / `latest_ts` from the head.
     - Walk the chain to find the newest version with `timestamp <= watermark_ts` (if any).
     - Set `stable_*` and `has_stable` accordingly.

2. **Keep this helper off hot paths initially**
   - Use it only in debug assertions and manual diagnostics in this phase.

**Verification:**

- By inspection: `inspect_versions` returns consistent results with existing `mvGET` and `mvGET_snapshot` behavior.

---

## 5. Phase 2 – Make GC Respect the Stable Version

**Objective:** Ensure `lazyReclaim` never removes the candidate stable version.

**Tasks:**

1. **Refactor `lazyReclaim`**
   - Current behavior:
     - Derives `watermark` from `retrieveShardW_relaxed() / 10`.
     - Walks the version chain and frees nodes below watermark.
   - New behavior:
     - Find the **newest** node in the chain with `timestamp <= watermark`.
     - Do **not** free that node.
     - Allow freeing strictly older nodes and re‑link the chain so that:
       - Any version older than the chosen stable node can be reclaimed.

2. **Add invariants**
   - After `lazyReclaim` runs:
     - Either no version exists with `timestamp <= watermark`, or
     - Exactly one “first” such version is preserved and reachable.

3. **Tie to `inspect_versions`**
   - In debug mode, `lazyReclaim` can call `inspect_versions(val, watermark)` and assert that:
     - If `has_stable`, the chain still contains that version after trimming.

**Verification:**

- Debug assertion: no key loses its only candidate stable version under GC.

---

## 6. Phase 3 – Expose Latest/Stable Helpers

**Objective:** Provide explicit helpers for latest and stable, and a promotion API.

**Tasks:**

1. **Add helpers in `MultiVersionValue`**

   ```cpp
   static bool get_latest(std::string& val, uint32_t& ts);
   static bool get_stable(std::string& val,
                          uint32_t watermark_ts,
                          uint32_t& stable_ts);

   static void promote_latest_to_stable_if_safe(std::string& val,
                                                uint32_t watermark_ts);
   ```

   - `get_latest`:
     - Reads the head and decodes `latest_ts` from the Node header.
   - `get_stable`:
     - Uses a similar walk as `inspect_versions` but optimized and optionally cached.
   - `promote_latest_to_stable_if_safe`:
     - If `latest_ts <= watermark_ts`, treat latest as the new logical stable candidate.
     - This can be realized by:
       - Ensuring GC preserves that node.
       - Optionally “collapsing” the chain when newer stable candidates supersede older ones.

2. **Keep promotion internal**
   - Do not change the `versioned_value` interface.
   - Promotion logic should be encapsulated inside `MultiVersionValue` and driven by watermark info.

**Verification:**

- Use `inspect_versions` in debug builds to check that `get_stable` aligns with the “newest ≤ watermark” definition.

---

## 7. Phase 4 – Hook Promotion into Watermark Advancement

**Objective:** Ensure stable slots are updated as the closed watermark moves forward.

**Tasks:**

1. **Identify core watermark update sites**
   - `sync_util::sync_logger::setSingleWatermark()`
   - `sync_util::sync_logger::computeLocal()` / `advancer()`
   - Any calls that raise `single_watermark_` or update `hist_timestamp`.

2. **Add a promotion hook API**
   - In `MultiVersionValue`, add:

     ```cpp
     static void on_watermark_advance(uint32_t new_watermark_ts);
     ```

   - This should be a **coarse‑grained** notification:
     - It can update a static/atomic “current watermark” used by `get_stable` / `promote_latest_to_stable_if_safe`.
     - It should not scan all keys by itself.

3. **Use promotion opportunistically**
   - Places that already touch values can opportunistically promote:
     - `mvInstall` (on writes/updates) can call `promote_latest_to_stable_if_safe` using the current watermark.
     - `lazyReclaim` can also enforce that the preserved node reflects the newest safe version.

**Verification:**

- Instrumentation: count how many promotions occur as watermarks advance; ensure no promotions happen when watermark is unchanged.

---

## 8. Phase 5 – Redirect Snapshot/Follower Reads to Stable

**Objective:** Make fast read‑only and follower reads consume only stable data.

**Tasks:**

1. **Refactor `mvGET_snapshot`**
   - Today, it walks the chain:
     - `header->timestamp <= snapshot_ts` → return.
     - Else, follow `header->data` / `data_size`.
   - New behavior:
     - Use `get_stable(val, snapshot_ts, stable_ts)` as the primary path.
     - Only fall back to explicit chain walking in debug mode (with logging) if `get_stable` fails, so we can detect gaps in the promotion logic.

2. **Update `MassTrans` snapshot paths**
   - In `src/mako/benchmarks/sto/MassTrans.hh`:
     - `transGet`, `transQuery`, and `transRQuery` already use `mvGET_snapshot` for read‑only fast‑path txns.
     - After refactoring `mvGET_snapshot`, these paths will automatically use the stable slot semantics.
   - Keep follower‑staleness gating (`should_redirect_to_leader` / `can_serve_follower_read`) unchanged.

**Verification:**

- Debug builds:
  - Log cases where `mvGET_snapshot` had to fall back to chain walking because no stable version was found.
  - Ensure such cases are rare or indicate missing promotion.

---

## 9. Phase 6 – Invariant Checking & Documentation

**Objective:** Provide strong evidence that the 2‑slot invariants hold and document the final design.

**Tasks:**

1. **Invariant checker**
   - Add a `debug_verify_two_slot_invariants(std::string val, uint32_t watermark_ts)` helper in `MultiVersionValue` that:
     - Walks the chain and confirms:
       - The head is the latest.
       - If any values exist with `timestamp <= watermark_ts`, the newest such value is treated as stable and preserved.
   - Call this helper in debug builds:
     - After `mvInstall`.
     - After `lazyReclaim`.
     - At the end of `mvGET_snapshot` when returning a stable value.

2. **Update `doc/read_only_fast_path.md` Phase 5**
   - Replace the current aspirational 2‑version MVCC description with a summary of the concrete implementation:
     - Latest vs stable semantics.
     - Relationship to `single_watermark_`.
     - How snapshot/follower reads and GC interact.

3. **Keep CI logic unchanged**
   - Do not add a dedicated CI microbenchmark for this epic.
   - Rely on existing replication tests and YCSB/TPCC experiments (when enabled) to exercise the path.

**Verification:**

- Code review focusing on invariants and debug assertions.
- Future experiments (YCSB/TPCC) can inspect promotion counters and snapshot read behavior using the instrumentation from Phase 0.

---

## 10. Summary

This epic converts Mako’s current “single‑chain” MVCC into a **dual‑version logical model** suitable for fast read‑only follower reads:

- Writes update the latest version as today.
- The system identifies and preserves a stable version per key based on the closed watermark.
- Snapshot and follower reads consume only the stable view, avoiding races with GC.

All major changes are localized to:

- `src/mako/benchmarks/sto/multiversion.hh`
- `src/mako/benchmarks/sto/sync_util.hh`
- `src/mako/benchmarks/sto/MassTrans.hh`
- `doc/read_only_fast_path.md`

so the rest of the codebase can treat the storage layer as providing a clean “latest”/“stable” abstraction without dealing with version chains directly.

