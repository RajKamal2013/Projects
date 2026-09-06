# SPEC — Sharding

Canonical contract for this in-process, sharded, log-structured key-value
store. If the code and this doc ever disagree, treat that as a bug to fix —
in whichever direction matches the invariants below.

## 1. Components

| Class | Package | Role |
|---|---|---|
| `FileObject` | `Models` | Fixed-size (2048B) serialized record: header + payload + checksum. The unit every append/read moves. |
| `SegmentId` | `Models` | Identity of a segment file, independent of whether it's open. |
| `Segment` | `Models` | One segment's lifecycle: `ACTIVE` → `SEALED` → `OBSOLETE`, plus the pin/refCount machinery reads use to guard against reclaim. |
| `Entry` | `Models` | Immutable index record: which segment + offset a key's value lives at, and whether it's `ACTIVE` or `DELETED` (tombstone). |
| `ShardId` | `Models` | Identity of a shard within a `ShardedKVStore`. |
| `FileStore` | `Store` | Owns one physical file per `SegmentId` under a shard's directory. All I/O is positional (`pwrite`/`pread`-equivalent), safe to call concurrently with no external locking. |
| `SegmentManager` | `Store` | Per-shard segment chain: exactly one `ACTIVE` segment, everything else `SEALED`/`OBSOLETE`. Owns the lock that makes "who's active" and "am I allowed to write to it" a single atomic question. |
| `Shard` | `Store` | One `SegmentManager` + one `FileStore` + one in-memory index (`ConcurrentHashMap<Integer, Entry>`). `put`/`get`/`delete`/`size`/`iterate`. |
| `ShardRouter` / `HashBasedShardRouter` | `Store` | Maps a key to the `ShardId` responsible for it. |
| `ShardedKVStore` | `Store` | Top level: `shardCount`, a `ShardRouter`, and `Map<ShardId, Shard>`. Every operation routes, looks up, delegates. |
| `OpStatus` | `Store` | Result of an append: success/failure + the offset written (or attempted). |

## 2. FileObject wire format

```
+--------+---------+-------+--------+----------+-----------------+
| MAGIC  | version | flags | length | checksum | payload (0-2032)|
| 4B     | 1B      | 1B    | 2B     | 4B(CRC32C)| + reserved 4B  |
+--------+---------+-------+--------+----------+-----------------+
```

- `BLOCK_SIZE = 2048`, `HEADER_SIZE = 16`, `MAX_PAYLOAD_SIZE = 2032`. Every
  record on disk is exactly `BLOCK_SIZE` bytes, zero-padded after the payload.
- Fixed size is what makes offset math trivial: `fileOffset = n * BLOCK_SIZE`.
- `flags` bit 0 (`FLAG_TOMBSTONE`) marks a delete record. `FLAG_COMPRESSED`
  and `FLAG_ENCRYPTED` are defined but unused — reserved for later.
- `fromBytes()` recomputes the CRC32C over the payload and throws
  `IllegalStateException` on mismatch — corruption is detected at read time,
  not silently returned.

## 3. On-disk layout

`ShardedKVStore.create(baseDir, shardCount)` lays out:

```
baseDir/
├── shard-0/
│   ├── segment-0.dat
│   ├── segment-1.dat
│   └── ...
├── shard-1/
│   └── ...
└── ...
```

Each shard's segment files are owned exclusively by that shard's `FileStore`
— nothing crosses shard directories. Within a segment file, records are
packed contiguously starting at offset 0, one `BLOCK_SIZE` slot per append.

## 4. Segment lifecycle

```
ACTIVE --markSealed()--> SEALED --markObsolete()--> OBSOLETE
```

- `ACTIVE → SEALED`: only `SegmentManager.roll()` does this, under its write
  lock (see §5). A segment accepts writes only while `ACTIVE`.
- `SEALED → OBSOLETE`: intended to be driven by compaction (**not
  implemented — V2**, see §7). `markObsolete()` and `awaitDrained()` are the
  primitives compaction must use:
  1. `segment.markObsolete()` — flips status, so `tryPinning()` starts
     rejecting new readers immediately.
  2. `segment.awaitDrained()` — blocks until every reader that pinned before
     step 1 has called `unpin()`.
  3. Only then is it safe to close/delete the segment's file.
- `tryPinning()` **increments `refCount` before checking status**, not
  after. This ordering is load-bearing: it guarantees that if compaction's
  drain loop ever observes `refCount == 0`, no reader can still be about to
  read from that segment — see the class javadoc on `Segment` for the full
  race argument. Do not reorder this to check-then-increment.

## 5. Concurrency model

| What | Mechanism | Guards against |
|---|---|---|
| Choosing a write offset within a segment file | Per-segment `AtomicLong.getAndAdd(BLOCK_SIZE)` in `FileStore` | Two writers landing on the same offset |
| The actual byte write/read | `FileChannel.write/read(buf, position)` (positional) | Nothing needed — JDK-guaranteed safe to call concurrently on one channel |
| "Which segment is active" + "am I allowed to write to it" | `SegmentManager.activeLock` (`ReentrantReadWriteLock`): `beginWrite()`/`endWrite()` take the read side, `roll()` takes the write side | The TOCTOU race where a writer reads the active segment, `roll()` seals it, and the writer's append lands in an already-sealed segment. `beginWrite()` reads `active` *while holding the lock*, so there's no gap for `roll()` to race into. |
| Write-order vs. index-publish-order for the same key | `Shard`'s 7 `ReentrantLock` stripes (`Math.floorMod(key, 7)`), held around `put`/`delete` | Two concurrent writes to the same key completing their `FileStore.append()`s in one order but publishing to the index in the other, letting an older write clobber a newer one |
| A reader vs. compaction reclaiming the segment it's about to read | `Segment.tryPinning()`/`unpin()`, called around every `FileStore.read()` in `Shard.get()` and the iterator | A read landing on a file that compaction already deleted |
| Reading the index itself | None needed | `ConcurrentHashMap.get()` is safe unlocked; `Entry` is immutable once published; segment bytes are never mutated once written (no offset reuse) |

**Write-then-publish, universally**: `Shard.index` is only ever updated
*after* the corresponding `FileStore.append()` succeeds. `delete()` follows
the same rule — it's a tombstone write, not an in-place map removal.

## 6. API surface

```java
// Shard
static Shard create(Path baseDir, long startingGeneration) throws IOException
OpStatus put(int key, FileObject obj)
OpStatus delete(int key)                          // tombstone write
FileObject get(int key)                           // null if absent/deleted/unreadable-right-now
int size()                                         // live (non-DELETED) key count, O(n)
Iterator<Map.Entry<Integer, FileObject>> iterate() // skips DELETED + un-pinnable entries

// SegmentManager
Segment beginWrite() / void endWrite()             // pair around a write
Segment roll()                                     // seal active, activate a fresh one
List<Segment> segments()

// FileStore
OpStatus append(SegmentId id, FileObject obj)
FileObject read(SegmentId id, long offset)
void close(SegmentId id) / void closeAll()

// ShardRouter
ShardId route(int key)                             // HashBasedShardRouter: floorMod(key, shardCount)

// ShardedKVStore
static ShardedKVStore create(Path baseDir, int shardCount) throws IOException
OpStatus put(int key, FileObject obj) / delete(int key) / FileObject get(int key)
Shard shard(ShardId id)
Iterator<Map.Entry<Integer, FileObject>> iterate() // chains every shard's iterator in turn
```

## 7. Explicit non-goals / known gaps (candidates for V2+)

These are deliberate or currently-accepted gaps, not oversights — flag them
if "fixing" one without updating this spec first.

- **Compaction is not implemented.** `markObsolete()`/`awaitDrained()`/
  `tryPinning()` exist as the primitives it needs, but nothing currently
  calls `markObsolete()`. Until compaction exists, segments accumulate
  forever.
- **No free-list / offset reuse.** A failed positional write permanently
  wastes that `BLOCK_SIZE` slot in the segment file — there's no reclaiming
  it. (Was implemented once, explicitly removed pending a future redesign.)
- **`SegmentManager.roll()` is manual.** No size/time-based auto-rolling
  policy exists yet.
- **`Shard.get()`'s retry loop can spin forever.** It `continue`s when
  `tryPinning()` fails, on the assumption that compaction will have updated
  the index to point at a replacement `Entry`. Since compaction doesn't
  exist yet, a segment that goes `OBSOLETE` while still referenced by an
  `Entry` will cause that key's `get()` to loop indefinitely.
- **No index persistence/recovery.** The `Entry` index is in-memory only.
  Restarting the process reopens segment files correctly (offsets resume
  from `FileChannel.size()`) but loses the entire key → offset mapping —
  there's no startup replay of segment contents to rebuild the index.
- **Single JVM only.** No cross-process concurrency, no network layer, no
  replication — this is an in-process store.
