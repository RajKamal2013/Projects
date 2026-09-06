# Sharding

A hand-rolled, in-process, sharded, log-structured key-value store — a
learning project for working through segment files, positional I/O,
tombstone deletes, and the concurrency invariants a real storage engine has
to get right (write-then-publish ordering, active-segment rotation racing
writers, readers racing compaction).

See [`SPEC.md`](SPEC.md) for the exact wire format, on-disk layout, and
concurrency contract — the source of truth if this README and the code ever
drift apart.

## Architecture

```
ShardedKVStore
├── ShardRouter (HashBasedShardRouter: key -> floorMod(key, shardCount))
└── Map<ShardId, Shard>
        │
        ▼
      Shard                                     (one per ShardId)
      ├── SegmentManager                        one ACTIVE segment,
      │     ├── Segment 0  SEALED                rest SEALED/OBSOLETE
      │     ├── Segment 1  SEALED
      │     └── Segment 2  ACTIVE  <- writes land here
      ├── FileStore                             one FileChannel per
      │     shard-N/segment-0.dat                SegmentId, positional
      │     shard-N/segment-1.dat                pwrite/pread
      │     shard-N/segment-2.dat
      └── ConcurrentHashMap<Integer, Entry>      key -> (segment, offset,
                                                   length, ACTIVE|DELETED)
```

Write path: `ShardedKVStore.put` → route to `Shard` → stripe lock (per key)
→ `SegmentManager.beginWrite()` (pins the active segment) →
`FileStore.append()` (positional write) → **only on success**, publish an
`Entry` into the index.

Read path: `ShardedKVStore.get` → route to `Shard` → index lookup (lock-free)
→ `Segment.tryPinning()` (guards against a concurrent reclaim) →
`FileStore.read()` → `unpin()`.

Delete is a write, not a removal: `delete()` appends a zero-payload tombstone
record and publishes an `Entry` with status `DELETED` — the key still shows
up in the underlying map, `get()`/`iterate()` just treat it as absent.

## Package layout

| Package | Contains |
|---|---|
| `org.project.Models` | Data types: `FileObject` (the 2KB block format), `Segment`, `SegmentId`, `Entry`, `ShardId` |
| `org.project.Store` | Engine: `FileStore`, `SegmentManager`, `Shard`, `ShardRouter`/`HashBasedShardRouter`, `ShardedKVStore`, `OpStatus` |

## Build

```bash
cd Sharding/Java/Sharding
./gradlew build
```

There's no test suite yet (`src/test` is currently empty) — everything so
far has been verified with ad-hoc concurrency stress tests during
development (see commit history / SPEC.md §7 for what's been exercised vs.
what hasn't).

## Usage

```java
ShardedKVStore store = ShardedKVStore.create(Path.of("/data/mystore"), 4);

store.put(42, FileObject.of("hello".getBytes(StandardCharsets.UTF_8)));
FileObject value = store.get(42);          // "hello"
store.delete(42);
store.get(42);                             // null

Iterator<Map.Entry<Integer, FileObject>> it = store.iterate();
while (it.hasNext()) {
    Map.Entry<Integer, FileObject> e = it.next();
    // e.getKey(), e.getValue().getPayload()
}
```

## What's next (see SPEC.md §7 for the full list)

- **Compaction** — reclaim `SEALED` segments once their live data has been
  rewritten elsewhere. The primitives (`markObsolete`, `awaitDrained`,
  `tryPinning`) already exist; nothing calls them yet.
- **Index recovery on restart** — segment files survive a restart, the
  in-memory index does not.
- **Auto-rolling policy** — `SegmentManager.roll()` is currently manual.
