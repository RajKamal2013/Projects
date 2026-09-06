package org.project.Store;

import org.project.Models.ShardId;

/** Maps a key to the shard responsible for it. */
public interface ShardRouter {
    ShardId route(int key);
}
