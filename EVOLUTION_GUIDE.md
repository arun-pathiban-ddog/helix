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
Current: 10 (champion: stage 2 latency)
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

## Structural / Algorithmic Changes (ENCOURAGED — not just parameter tuning)

The "Known Performance Levers" above are the *easy* wins (tuning a single constant).
They are largely exhausted. To keep improving, you SHOULD also propose **structural
and algorithmic** changes inside the EVOLVE-SAFE files — these often unlock larger,
more durable gains than nudging a number. Treat the constants as a fallback, not the
default.

Examples of the *kind* of change to consider (illustrative, not a checklist):
- **Data-structure swaps** — e.g. replace a linear scan / `Vec` lookup in the produce
  or replication hot path with a hashmap/index; use a ring buffer instead of repeated
  allocation; intern repeated keys.
- **Algorithmic restructuring** — e.g. coalesce/merge work that is currently done
  per-message into per-batch; change the batcher flush from a fixed timer to an
  adaptive/event-driven trigger; pipeline a step that is currently serial.
- **Reducing syscalls / copies / allocations** — e.g. reuse buffers across requests,
  vectorize writes, avoid an intermediate copy in the encode/decode path.
- **Concurrency-shape changes** — e.g. move a blocking step off the request path,
  batch lock acquisitions, replace a per-request lock with a sharded one.

Rules for structural changes:
- Stay within the EVOLVE-SAFE file list. A cohesive change MAY span several
  EVOLVE-SAFE files if they belong to the same mechanism.
- The WAL `sync()` invariant and the `helix-runtime` / INVARIANT files still apply.
- It must still pass the WAL durability DST and `cargo check -p helix-server`.
- Prefer one cohesive structural change over a scattershot refactor — keep the diff
  reviewable and tied to a single named mechanism.

When you form a hypothesis, deliberately vary the *class* of change across variants:
do not propose another constant tweak if the last few attempts were all constant
tweaks. Check the Champion History below — if it is dominated by parameter changes,
that is a signal to try a structural/algorithmic one next.

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

Only ONE cohesive change per response, tied to a single named mechanism. The change
may be a parameter tweak OR a structural/algorithmic change (see "Structural /
Algorithmic Changes" above) — the latter is encouraged when the parameter levers are
exhausted. "Cohesive" does not mean "one line": a structural change may touch several
EVOLVE-SAFE files if they implement the same mechanism. Do not refactor code unrelated
to your hypothesis.

---

## Champion History

- Stage 1 (latency): evolve(latency s1.v0): Reduced batcher default linger_ms from 9ms to 5ms in BatcherConfig::default() to decrease tail latency by cutting worst-case batching delay by 7ms → +97.5%
- Stage 1 (latency): Increased MAX_INFLIGHT_APPEND_ENTRIES from 5 to 10 in helix-raft/src/lib.rs to enable deeper Raft replication pipelining and reduce p95 latency → +87.0%
- Stage 1 (latency): Reduced batcher default linger_ms from 5ms to 2ms to lower tail latency by flushing batches sooner → +95.4%
- Stage 2 (latency): Increased MAX_INFLIGHT_APPEND_ENTRIES from 5 to 10 in helix-raft/src/lib.rs to allow deeper Raft pipelining for reduced p95 latency → +91.4%