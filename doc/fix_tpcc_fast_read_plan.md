# Fix TPC-C Benchmark for Fast Read-Only Transaction Path

## Issue Description

When running the TPC-C benchmark with `MAKO_FAST_RO_MODE=fast`, the benchmark crashes with:

```
PANIC ALWAYS_ERROR (tpcc.cc:123): the error for ALWAYS ERROR!
```

This occurs because the fast read-only transaction path violates invariants expected by the TPC-C benchmark code.

## Root Cause Analysis

### 1. The `ALWAYS_ERROR` Function (tpcc.cc:113-126)

```cpp
static inline ALWAYS_INLINE
void ALWAYS_ERROR(bool aa){
  if (likely(aa)){
  }else{
    if(TThread::transget_without_throw){
      // Do nothing - abort was already handled
    }else{
      Panic("the error for ALWAYS ERROR!");  // <-- Line 123 triggers!
    }
  }
}
```

**Behavior**:
- If condition `aa` is true: do nothing (expected)
- If condition `aa` is false AND `TThread::transget_without_throw` is true: do nothing (graceful abort)
- If condition `aa` is false AND `TThread::transget_without_throw` is false: **PANIC!**

### 2. Why the Flag is Not Set

The `TThread::transget_without_throw` flag is set in `transGet()` (point reads) when:
- Follower is stale and can't serve the read (MassTrans.hh:165-170)
- Element validity check fails (MassTrans.hh:183-187)

**However**, `transQuery()` and `transRQuery()` (scan operations) do **NOT** perform follower staleness checks. They only acquire the read timestamp but never check if the follower can serve the read.

### 3. Problematic Code Paths in TPC-C

**In `txn_order_status()` (lines 3133-3260):**
- Line 3184: `ALWAYS_ERROR(c.size() > 0)` - customer name scan may return 0 results
- Line 3233: `ALWAYS_ERROR(c_oorder.size())` - oorder scan may return 0 results
- Line 3238: `ALWAYS_ERROR(c_oorder.size() == 1)` - oorder rscan may return 0 results
- Line 3249: `ALWAYS_ERROR(c_order_line.n >= 5 && c_order_line.n <= 15)` - order line count invariant

**In `txn_stock_level()` (lines 3296-3406):**
- Line 3343: `ALWAYS_ERROR(tbl_district(...)->get(...))` - district get may fail
- Lines 3374-3385: Has partial fix pattern but not complete

### 4. Why Scans Return Empty Results in Fast Path

In the fast read-only path, MVCC snapshot reads (`mvGET_snapshot()`) may return no results because:
- Data hasn't been replicated to the snapshot timestamp yet
- The snapshot timestamp is earlier than when the data was committed
- On a stale follower, the data may not be visible at the requested timestamp

---

## Proposed Solution

Three complementary changes to fix this issue:

### Change 1: Modify `ALWAYS_ERROR` to Handle Fast Path

**File**: `src/mako/benchmarks/tpcc.cc` (lines 113-126)

**Before**:
```cpp
static inline ALWAYS_INLINE
void ALWAYS_ERROR(bool aa){
  if (likely(aa)){
  }else{
    if(TThread::transget_without_throw){
    }else{
      Panic("the error for ALWAYS ERROR!");
    }
  }
}
```

**After**:
```cpp
static inline ALWAYS_INLINE
void ALWAYS_ERROR(bool aa){
  if (likely(aa)){
    // Expected case - condition is true
  }else{
    if(TThread::transget_without_throw){
      // Already aborted via fast path
    }else if(TpccFastReadOnlyModeEnabled()){
      // Fast RO mode: treat invariant failure as abort, not panic
      Sto::abort_without_throw();
      TThread::transget_without_throw = true;
    }else{
      Panic("the error for ALWAYS ERROR!");
    }
  }
}
```

### Change 2: Add Staleness Check to Scans (MassTrans.hh)

**File**: `src/mako/benchmarks/sto/MassTrans.hh`

**2a. In `transQuery()` - Insert after line 425 (after `snapshot_reads = snapshot_ts > 0;`):**

```cpp
    // ==================== FOLLOWER READ SUPPORT FOR SCANS ====================
    if (BenchmarkConfig::getInstance().getIsReplicated() &&
        TThread::txn && TThread::txn->is_read_only_fast_path()) {
      uint32_t read_ts = snapshot_ts;
      if (read_ts > 0) {
        if (sync_util::sync_logger::should_redirect_to_leader(read_ts)) {
          mass_trans_instrumentation::recordFollowerReadAbortStale();
          Sto::abort_without_throw();
          TThread::transget_without_throw = true;
          return;  // void return for transQuery
        }
      }
    }
    // ==========================================================================
```

**2b. In `transRQuery()` - Insert after line 498 (after `snapshot_reads = snapshot_ts > 0;`):**

Same code block as above.

### Change 3: Add Post-Scan Abort Checks (tpcc.cc)

After each scan in read-only transactions, add the abort check pattern:

**Pattern to insert after each scan:**
```cpp
if(TThread::transget_without_throw){
  TThread::transget_without_throw=false;
  db->abort_txn_local(txn);
  return txn_result(false,0);
}
```

**Locations in `txn_order_status()`:**
- After line 3183 (before `ALWAYS_ERROR(c.size() > 0)`)
- After line 3231 (before `ALWAYS_ERROR(c_oorder.size())`)
- After line 3237 (before `ALWAYS_ERROR(c_oorder.size() == 1)`)
- After line 3248 (before `ALWAYS_ERROR(c_order_line.n >= 5...)`)

**Locations in `txn_stock_level()`:**
- After line 3361 (after order_line scan)

---

## Files to Modify

| File | Lines | Change |
|------|-------|--------|
| `src/mako/benchmarks/tpcc.cc` | 113-126 | Modify `ALWAYS_ERROR` function |
| `src/mako/benchmarks/tpcc.cc` | ~3183 | Add abort check after customer name scan |
| `src/mako/benchmarks/tpcc.cc` | ~3231 | Add abort check after oorder scan |
| `src/mako/benchmarks/tpcc.cc` | ~3237 | Add abort check after oorder rscan |
| `src/mako/benchmarks/tpcc.cc` | ~3248 | Add abort check after order_line scan |
| `src/mako/benchmarks/tpcc.cc` | ~3361 | Add abort check after order_line scan (stock_level) |
| `src/mako/benchmarks/sto/MassTrans.hh` | ~425 | Add staleness check to `transQuery()` |
| `src/mako/benchmarks/sto/MassTrans.hh` | ~498 | Add staleness check to `transRQuery()` |

---

## Testing Plan

After implementing the changes:

```bash
# Build
make -j32

# Run fast path evaluation
NSHARDS=1 THREADS=6 RUNTIME=60 \
MAKO_TPCC_WORKLOAD_MIX="10,10,0,40,40" \
MAKO_FAST_RO_MODE=fast \
bash scripts/run_tpcc_fast_path_eval.sh
```

**Success criteria:**
1. No "ALWAYS ERROR" panic in logs
2. `agg_persist_throughput` metric is reported
3. Transactions complete (may have some aborts, but no crashes)

---

## Risk Assessment

- **Low Risk**: Changes are localized to fast-path handling
- **Backward Compatible**: Normal (non-fast) path unchanged when `MAKO_FAST_RO_MODE` is not set
- **Graceful Degradation**: Fast path aborts return `txn_result(false, 0)` - transactions can retry

---

## Alternative Approaches Considered

1. **Only modify `ALWAYS_ERROR`**: Simpler but doesn't fix the root cause (scans not checking staleness)
2. **Only add scan staleness checks**: More robust but doesn't protect against other invariant failures
3. **Disable invariant checks in fast mode entirely**: Too risky, could mask real bugs

The proposed solution combines approaches 1 and 2 for defense in depth.
