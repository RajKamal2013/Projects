package org.project.Store;

import org.project.Models.Segment;
import org.project.Models.SegmentId;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Per-shard segment chain: exactly one ACTIVE segment at a time, everything
 * else SEALED. One lock guards "which segment is active, and is anyone
 * currently writing to it" as a single unit: beginWrite() returns whichever
 * segment is active *while holding the read side*, so there's no gap between
 * "see the active segment" and "have permission to write to it" for roll()
 * to race into. roll() takes the write side, so it can't swap/seal out from
 * under an in-flight write, and acquiring it is itself the drain.
 *
 * `active` is a plain field, not volatile/atomic — every access to it goes
 * through activeLock (in beginWrite() or roll()), so the lock alone provides
 * the visibility guarantee.
 */
public final class SegmentManager {
    private final List<Segment> segments = new CopyOnWriteArrayList<>();
    private Segment active;
    private final AtomicLong nextId;
    private final ReentrantReadWriteLock activeLock = new ReentrantReadWriteLock();

    public SegmentManager(long startingGeneration) {
        Segment first = new Segment(new SegmentId(startingGeneration), startingGeneration);
        this.segments.add(first);
        this.active = first;
        this.nextId = new AtomicLong(startingGeneration + 1);
    }

    /** Acquires shared write access and returns the active segment. Pair with endWrite(). */
    public Segment beginWrite() {
        activeLock.readLock().lock();
        return active;
    }

    public void endWrite() {
        activeLock.readLock().unlock();
    }

    /** Seals the current active segment and opens the next one as active. */
    public Segment roll() {
        activeLock.writeLock().lock();
        try {
            Segment current = active;
            current.markSealed();
            long id = nextId.getAndIncrement();
            Segment next = new Segment(new SegmentId(id), id);
            segments.add(next);
            active = next;
            return next;
        } finally {
            activeLock.writeLock().unlock();
        }
    }

    public List<Segment> segments() {
        return List.copyOf(segments);
    }
}
