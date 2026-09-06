package org.project.Store;

import org.project.Models.FileObject;
import org.project.Models.ShardId;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Top-level store: SHARD_COUNT shards, a router that maps a key to a ShardId,
 * and a Map<ShardId, Shard> holding each shard's own SegmentManager+FileStore.
 * Every operation is: route(key) -> ShardId -> look up its Shard -> delegate.
 */
public final class ShardedKVStore {
    private final int shardCount;
    private final ShardRouter router;
    private final Map<ShardId, Shard> shards;

    public ShardedKVStore(int shardCount, ShardRouter router, Map<ShardId, Shard> shards) {
        this.shardCount = shardCount;
        this.router = router;
        this.shards = shards;
    }

    /** Creates a fresh store: shardCount Shards, each rooted at baseDir/shard-<i>. */
    public static ShardedKVStore create(Path baseDir, int shardCount) throws IOException {
        ShardRouter router = new HashBasedShardRouter(shardCount);
        Map<ShardId, Shard> shards = new ConcurrentHashMap<>();
        for (int i = 0; i < shardCount; i++) {
            Shard shard = Shard.create(baseDir.resolve("shard-" + i), 0);
            shards.put(new ShardId(i), shard);
        }
        return new ShardedKVStore(shardCount, router, shards);
    }

    public OpStatus put(int key, FileObject obj) {
        return shardFor(key).put(key, obj);
    }

    public OpStatus delete(int key) {
        return shardFor(key).delete(key);
    }

    public FileObject get(int key) {
        return shardFor(key).get(key);
    }

    private Shard shardFor(int key) {
        ShardId id = router.route(key);
        Shard shard = shards.get(id);
        if (shard == null) {
            throw new IllegalStateException("no shard for id " + id.value());
        }
        return shard;
    }

    public int shardCount() {
        return shardCount;
    }

    public Shard shard(ShardId id) {
        return shards.get(id);
    }

    /** Iterates every shard's index in turn — exhausts one shard fully before moving to the next. */
    public Iterator<Map.Entry<Integer, FileObject>> iterate() {
        return new ChainedIterator(shards.values().iterator());
    }

    private static final class ChainedIterator implements Iterator<Map.Entry<Integer, FileObject>> {
        private final Iterator<Shard> shardIterator;
        private Iterator<Map.Entry<Integer, FileObject>> current = Collections.emptyIterator();

        ChainedIterator(Iterator<Shard> shardIterator) {
            this.shardIterator = shardIterator;
        }

        @Override
        public boolean hasNext() {
            while (!current.hasNext() && shardIterator.hasNext()) {
                current = shardIterator.next().iterate();
            }
            return current.hasNext();
        }

        @Override
        public Map.Entry<Integer, FileObject> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return current.next();
        }
    }
}
