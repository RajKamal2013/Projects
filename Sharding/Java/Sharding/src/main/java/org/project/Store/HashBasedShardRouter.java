package org.project.Store;

import org.project.Models.ShardId;

/** key % shardCount, via Math.floorMod so negative keys still land on a valid shard. */
public final class HashBasedShardRouter implements ShardRouter {
    private final int shardCount;

    public HashBasedShardRouter(int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        this.shardCount = shardCount;
    }

    @Override
    public ShardId route(int key) {
        return new ShardId(Math.floorMod(key, shardCount));
    }
}
