package org.project.Store;

import org.project.Models.Entry;
import org.project.Models.FileObject;
import org.project.Models.Segment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One shard = one SegmentManager (which segment is active) + one FileStore
 * (the bytes) + one in-memory index (key -> where those bytes live).
 *
 * The index is only ever updated *after* FileStore.append succeeds — a reader
 * must never be able to look up a key and land on an offset that isn't
 * durably written yet. delete() follows the same rule: it's a write too (a
 * tombstone record), not an in-place removal from the index.
 *
 * ConcurrentHashMap alone protects the map's own structure, but not the
 * ordering of what ends up in it: two concurrent put()s (or a put() racing a
 * delete()) on the same key can have their fileStore.append()s complete in
 * one order and their index.put() publishes land in the other, letting an
 * older write clobber a newer one. A fixed set of stripe locks, one per
 * key % STRIPE_COUNT, serializes put/delete for a given key (or whatever else
 * happens to share its stripe) without serializing unrelated keys. get()
 * doesn't need it — ConcurrentHashMap.get() is safe unlocked, Entry is
 * immutable once published, and the bytes it points at are already durably
 * written and never mutated.
 */
public final class Shard {
    private static final int STRIPE_COUNT = 7;

    private final SegmentManager segmentManager;
    private final FileStore fileStore;
    private final Map<Integer, Entry> index = new ConcurrentHashMap<>();
    private final ReentrantLock[] stripes = new ReentrantLock[STRIPE_COUNT];

    public Shard(SegmentManager segmentManager, FileStore fileStore) {
        this.segmentManager = segmentManager;
        this.fileStore = fileStore;
        for (int i = 0; i < STRIPE_COUNT; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    private ReentrantLock stripeFor(int key) {
        return stripes[Math.floorMod(key, STRIPE_COUNT)];
    }

    /** Creates a fresh shard: a new FileStore rooted at baseDir, one ACTIVE segment. */
    public static Shard create(Path baseDir, long startingGeneration) throws IOException {
        FileStore fileStore = new FileStore(baseDir);
        SegmentManager segmentManager = new SegmentManager(startingGeneration);
        return new Shard(segmentManager, fileStore);
    }

    public OpStatus put(int key, FileObject obj) {
        ReentrantLock stripe = stripeFor(key);
        stripe.lock();
        try {
            Segment active = segmentManager.beginWrite();
            try {
                OpStatus status = fileStore.append(active.getId(), obj);
                if (status.isSuccess()) {
                    index.put(key, new Entry(active, status.offset(), obj.size()));
                }
                return status;
            } finally {
                segmentManager.endWrite();
            }
        } finally {
            stripe.unlock();
        }
    }

    /** Tombstones a key: appends a zero-payload tombstone record, then publishes a DELETED entry. */
    public OpStatus delete(int key) {
        ReentrantLock stripe = stripeFor(key);
        stripe.lock();
        try {
            Segment active = segmentManager.beginWrite();
            try {
                FileObject tombstone = new FileObject(FileObject.CURRENT_VERSION, FileObject.FLAG_TOMBSTONE, new byte[0]);
                OpStatus status = fileStore.append(active.getId(), tombstone);
                if (status.isSuccess()) {
                    index.put(key, new Entry(active, status.offset(), tombstone.size(), Entry.Status.DELETED));
                }
                return status;
            } finally {
                segmentManager.endWrite();
            }
        } finally {
            stripe.unlock();
        }
    }

    public FileObject get(int key) {
        while (true) {
            Entry entry = index.get(key);

            if (entry == null || entry.getStatus() == Entry.Status.DELETED) {
                return null;
            }

            Segment segment = entry.getSegment();

            if (!segment.tryPin()) {
                // Segment became obsolete.
                // Re-read the index because compaction may have
                // replaced the Entry with a new segment.
                continue;
            }

            FileObject obj;
            try {
                obj = fileStore.read(segment.getId(), entry.getOffset());
            } finally {
                segment.unpin();
            }
            if (obj == null) {
                throw new IllegalStateException(
                        "fileStore.read() returned null for segment " + segment.getId().value()
                                + " offset " + entry.getOffset());
            }
            return obj;
        }
    }

    /**
     * Number of live (non-DELETED) keys currently in this shard's index.
     * O(n) — delete() leaves tombstoned entries in the map rather than
     * removing them, so a plain index.size() would over-count.
     */
    public int size() {
        int count = 0;
        for (Entry entry : index.values()) {
            if (entry.getStatus() != Entry.Status.DELETED) {
                count++;
            }
        }
        return count;
    }

    public SegmentManager segmentManager() {
        return segmentManager;
    }

    public FileStore fileStore() {
        return fileStore;
    }

    /** Iteration over the index: skips DELETED and un-pinnable entries. */
    public Iterator<Map.Entry<Integer, FileObject>> iterate() {
        return new IndexIterator(index.entrySet().iterator());
    }

    /**
     * Look-ahead iterator: hasNext() does the real work (scan the underlying
     * index iterator, skip DELETED entries, skip entries whose segment can't
     * be pinned, otherwise pin -> read -> unpin and cache one ready result);
     * next() just hands back whatever hasNext() already prepared.
     */
    private final class IndexIterator implements Iterator<Map.Entry<Integer, FileObject>> {
        private final Iterator<Map.Entry<Integer, Entry>> underlying;
        private Map.Entry<Integer, FileObject> prepared;

        IndexIterator(Iterator<Map.Entry<Integer, Entry>> underlying) {
            this.underlying = underlying;
        }

        @Override
        public boolean hasNext() {
            if (prepared != null) {
                return true;
            }
            while (underlying.hasNext()) {
                Map.Entry<Integer, Entry> next = underlying.next();
                Entry entry = next.getValue();
                if (entry.getStatus() == Entry.Status.DELETED) {
                    continue;
                }
                Segment segment = entry.getSegment();
                if (!segment.tryPin()) {
                    continue;
                }
                FileObject obj;
                try {
                    obj = fileStore.read(segment.getId(), entry.getOffset());
                } finally {
                    segment.unpin();
                }
                if (obj == null) {
                    throw new IllegalStateException(
                            "fileStore.read() returned null for segment " + segment.getId().value()
                                    + " offset " + entry.getOffset());
                }
                prepared = Map.entry(next.getKey(), obj);
                return true;
            }
            return false;
        }

        @Override
        public Map.Entry<Integer, FileObject> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<Integer, FileObject> result = prepared;
            prepared = null;
            return result;
        }
    }
}
