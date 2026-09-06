package org.project.Store;

/** Result of a FileStore.append call: did it land, and at what offset. */
public final class OpStatus {
    public enum Result {
        SUCCESS,
        FAILURE
    }

    private final Result result;
    private final long offset;
    private final Throwable error;

    private OpStatus(Result result, long offset, Throwable error) {
        this.result = result;
        this.offset = offset;
        this.error = error;
    }

    public static OpStatus success(long offset) {
        return new OpStatus(Result.SUCCESS, offset, null);
    }

    public static OpStatus failure(long offset, Throwable error) {
        return new OpStatus(Result.FAILURE, offset, error);
    }

    public boolean isSuccess() {
        return result == Result.SUCCESS;
    }

    /** Offset the payload was written to (success) or attempted at (failure). */
    public long offset() {
        return offset;
    }

    /** Null on success. */
    public Throwable error() {
        return error;
    }
}
