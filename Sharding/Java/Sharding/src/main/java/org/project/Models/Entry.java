package org.project.Models;

/**
 * Immutable index record: where a key's value lives (which segment, at what
 * offset/length) and whether it's live or tombstoned. Shard's index maps
 * key -> Entry; a DELETED entry still points at a real (zero-payload
 * tombstone) record in the log, it just means the key reads as absent.
 */
public final class Entry {
    private final Segment segment;
    private final long offset;
    private final int length;
    private final Status status;
    public enum Status {
        ACTIVE,
        DELETED
    }

    public Entry(Segment segment, long offset, int length) {
        this(segment, offset, length, Status.ACTIVE);
    }

    public Entry(Segment segment, long offset, int length, Status status) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
        this.status = status;
    }

    public Segment getSegment() {
        return segment;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public Status getStatus() {
        return status;
    }
}
