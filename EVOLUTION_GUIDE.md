# Helix Evolution Guide

This file is read by the live evolution loop's mutation agent (Claude) to scope
what changes are safe, what performance levers exist, and what must never be touched.

---

## Codebase Map

| Crate | Role | Performance relevance |
|---|---|---|
| `helix-server` | Kafka broker: connection handling, batcher, request dispatch | **HIGH** — batcher flush logic is the primary produce-latency lever |
| `helix-raft` | Raft consensus: log replication, pipelining, leader election | **HIGH** — inflight cap and batch size control replication throughput |
| `helix-wal` | Write-ahead log: fsync, segment rotation, durability | **CRITICAL INVARIANT** — WAL fsync must never be skipped (DST catches it) |
| `helix-flow` | Back-pressure: AIMD controller, token bucket, fair queue | **MEDIUM** — flow thresholds affect throughput under burst load |
| `helix-routing` | Topic/partition routing, leader lookup | LOW |
| `helix-core` | Shared types, errors, codec | LOW |
| `helix-progress` | Progress tracking (commit index) | LOW |
| `helix-runtime` | Async runtime + deterministic simulation substrate | **DO NOT MODIFY** — changes break DST |
| `helix-tier` | Storage tiering | LOW |
| `helix-workload` | Load-test producer/consumer helpers | LOW |

---

## Files Safe to Modify (EVOLVE-SAFE)

You may change any file in this list. Stay within the file — do not add new files
unless the change is clearly scoped and tested.

```
helix-server/src/service/batcher.rs      ← linger_ms, flush thresholds, batch coalescing
helix-server/src/service/mod.rs          ← connection handling, request dispatch
helix-server/src/service/producer.rs     ← produce path, back-pressure interaction
helix-raft/src/lib.rs                    ← MAX_INFLIGHT_APPEND_ENTRIES, APPEND_ENTRIES_BATCH_SIZE_MAX
helix-raft/src/replication.rs            ← replication loop timing, pipeline depth
helix-raft/src/leader.rs                 ← leader-side batching
helix-flow/src/aimd.rs                   ← AIMD window size, step-up/down thresholds
helix-flow/src/token_bucket.rs           ← rate limiting parameters
helix-flow/src/fair_queue.rs             ← fair scheduling weights
helix-wal/src/wal.rs                     ← SAFE: buffer sizes, rotation thresholds
                                           INVARIANT: never modify or remove active.file.sync().await
```

---

## Files INVARIANT (do not modify)

```
helix-runtime/**           ← simulation substrate; DST uses it for deterministic crash injection
helix-core/src/error.rs   ← protocol error codes; changing breaks wire compatibility
helix-server/src/main.rs  ← entrypoint; changing breaks Docker build / flag parsing
helix-wal/src/wal.rs      ← active.file.sync().await block is INVARIANT (see below)
```

---

## Known Performance Levers

### 1. Batcher linger_ms — `helix-server/src/service/batcher.rs`
```
Pattern: .unwrap_or(<N>)  in the HELIX_BATCHER_LINGER_MS env-var read
Effect:  lower → less produce latency; higher → more throughput via coalescing
Range:   1 ms (floor) to ~50 ms (beyond which burst throughput plateaus)
Current: 2 ms (champion: evolve(latency s1.v0))
```

### 2. MAX_INFLIGHT_APPEND_ENTRIES — `helix-raft/src/lib.rs`
```
Pattern: MAX_INFLIGHT_APPEND_ENTRIES: u32 = <N>
Effect:  higher → deeper replication pipelining → more throughput
Range:   5 (default) to ~30; diminishing returns after 20
```

### 3. APPEND_ENTRIES_BATCH_SIZE_MAX — `helix-raft/src/lib.rs`
```
Pattern: APPEND_ENTRIES_BATCH_SIZE_MAX: u32 = <N>
Effect:  higher → fewer round-trips per burst → higher throughput
Range:   1000 (default) to ~8000
```

### 4. WAL buffer / rotation threshold — `helix-wal/src/wal.rs`
```
Effect:  larger buffer → fewer fsync calls per second → less CPU overhead
CAUTION: do not change the sync() call itself — only rotation/buffer size constants
```

### 5. Flow control thresholds — `helix-flow/src/aimd.rs`, `token_bucket.rs`
```
Effect:  relaxing thresholds reduces back-pressure stalls under burst load
CAUTION: too permissive → producer overload → replication lag spike
```

---

## Control Plane Mutations (also allowed)

Alongside code changes, you may also propose **queue topology changes** using the
Temper API. These are expressed as instructions in your response (NOT code edits)
and the live loop applies them via governed Temper actions.

Allowed:
- **Create new queues** — all names MUST start with `shopping_store` prefix
  Example: split `shopping_store` into `shopping_store_priority` + `shopping_store_bulk`
- **Propose Producer/Consumer configuration changes** — e.g. increase concurrency
  on a high-priority queue, decrease on a low-priority one

NOT allowed:
- Rename or delete existing `shopping_store*` queues
- Create queues without the `shopping_store` prefix
- Modify queue names already in use by running workloads

---

## Safety Gate

**ALL code mutations must pass:**
```
cargo test -p helix-tests --lib test_dst_shared_wal_basic_durability
```

This is the WAL durability DST. It runs in ~2-3 minutes and catches any mutation
that breaks crash-safety. The live loop runs this before any Cloud Build.

Control plane mutations (queue/topology changes) skip the DST (no code change).

---

## WAL Invariant — NEVER modify this block

The following block in `helix-wal/src/wal.rs` inside `pub async fn sync()` is
**INVARIANT** and must never be removed, commented out, or changed:

```rust
        // Sync the active segment.
        if let Some(active) = &self.active_segment {
            let result = active.file.sync().await;
            debug!(
                bytes = self.bytes_since_sync,
                success = result.is_ok(),
                "Syncing active segment"
            );
            result?;
        }
```

Removing this causes a durability violation that the DST (`test_dst_shared_wal_basic_durability`)
will detect and reject. This is the proven cull from the lethal-mutation spike.

---

## Mutation Format

Return your mutation as a JSON object in this exact format:

```json
{
  "change_description": "One sentence describing what you changed and why",
  "rationale": "2-3 sentences explaining why this change should improve the target metric",
  "mutation_type": "code",
  "edits": [
    {
      "file": "relative/path/from/helix/root.rs",
      "old": "exact string to replace (must match verbatim)",
      "new": "replacement string"
    }
  ],
  "control_plane": []
}
```

For control plane mutations (queue topology changes), set `mutation_type` to `"control_plane"`,
leave `edits` empty, and populate `control_plane`:

```json
{
  "change_description": "Split shopping_store into priority and bulk sub-queues",
  "rationale": "...",
  "mutation_type": "control_plane",
  "edits": [],
  "control_plane": [
    {"action": "create_queue", "name": "shopping_store_priority", "partitions": 3},
    {"action": "create_queue", "name": "shopping_store_bulk", "partitions": 3}
  ]
}
```

Only ONE cohesive change per response. If you propose a code change, make it
minimal and targeted — do not refactor unrelated code.

---

## Champion History

- Stage 1 (latency): evolve(latency s1.v0): Reduced batcher default linger_ms from 9ms to 2ms in BatcherConfig::default() to decrease tail latency by cutting worst-case batching delay by 7ms → +97.5%