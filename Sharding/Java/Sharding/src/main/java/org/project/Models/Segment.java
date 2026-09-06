package org.project.Models;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Segment {
    public enum Status {
        ACTIVE,
        SEALED,
        OBSOLETE
    }

    private final SegmentId id;
    private final AtomicReference<Status> status;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final long generation;

    public Segment(SegmentId id, long generation) {
        this.id = id;
        this.generation = generation;
        this.status = new AtomicReference<>(Status.ACTIVE);
    }

    public SegmentId getId() {
        return id;
    }

    public Status getStatus() {
        return status.get();
    }

    /**
     * Registers one in-flight reader against reclaim, unless already OBSOLETE.
     * Must be paired with unpin() (a finally block). Increments *before*
     * checking status: status only ever moves forward to OBSOLETE and never
     * back, and compaction always finishes markObsolete() before it starts
     * waiting on refCount — so if this check sees OBSOLETE, reclaim may
     * already be waiting/running and the caller must not touch the file; if it
     * doesn't, compaction's drain loop is guaranteed to observe this increment
     * and wait for unpin() before reclaiming.
     */
    public boolean tryPin() {
        refCount.incrementAndGet();
        if (status.get() == Status.OBSOLETE) {
            refCount.decrementAndGet();
            return false;
        }
        return true;
    }

    public void unpin() {
        refCount.decrementAndGet();
    }

    public boolean markObsolete() {
        return status.compareAndSet(Status.SEALED, Status.OBSOLETE);
    }

    /** Blocks until every reader that pinned before reclaim began has unpinned. */
    public void awaitDrained() {
        while (refCount.get() > 0) {
            Thread.onSpinWait();
        }
    }

    public boolean markSealed() {
        return status.compareAndSet(Status.ACTIVE, Status.SEALED);
    }
}
