package org.project.Store;

import org.project.Models.FileObject;
import org.project.Models.SegmentId;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns one physical file per SegmentId under baseDir. All I/O is positional
 * (FileChannel.write/read with an explicit offset), which the JDK guarantees
 * is safe to call concurrently on the same channel from multiple threads with
 * no external locking — each call maps to a single pwrite(2)/pread(2). The
 * only thing that needs to be atomic is *choosing* the offset, which is a
 * lock-free AtomicLong.getAndAdd per segment.
 */
public final class FileStore {

    private final Path baseDir;
    private final Map<SegmentId, SegmentFile> segmentFiles = new ConcurrentHashMap<>();

    public FileStore(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    public OpStatus append(SegmentId id, FileObject obj) {
        SegmentFile sf = segmentFiles.computeIfAbsent(id, this::openSegmentFile);

        long offset = sf.nextOffset.getAndAdd(FileObject.BLOCK_SIZE);

        ByteBuffer buf = ByteBuffer.wrap(obj.toBytes());
        long position = offset;
        try {
            while (buf.hasRemaining()) {
                int n = sf.channel.write(buf, position);
                if (n < 0) {
                    throw new EOFException("unexpected EOF writing segment " + id.value());
                }
                position += n;
            }
            return OpStatus.success(offset);
        } catch (IOException e) {
            return OpStatus.failure(offset, e);
        }
    }

    public FileObject read(SegmentId id, long offset) {
        SegmentFile sf = segmentFiles.get(id);
        if (sf == null) {
            throw new IllegalStateException("no open file for segment " + id.value());
        }
        if (offset < 0 || offset % FileObject.BLOCK_SIZE != 0 || offset >= sf.nextOffset.get()) {
            throw new IllegalArgumentException(
                    "offset " + offset + " out of range for segment " + id.value());
        }

        ByteBuffer buf = ByteBuffer.allocate(FileObject.BLOCK_SIZE);
        long position = offset;
        try {
            while (buf.hasRemaining()) {
                int n = sf.channel.read(buf, position);
                if (n < 0) {
                    throw new EOFException(
                            "unexpected EOF reading segment " + id.value() + " at offset " + offset);
                }
                position += n;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return FileObject.fromBytes(buf.array());
    }

    public void close(SegmentId id) throws IOException {
        SegmentFile sf = segmentFiles.remove(id);
        if (sf != null) {
            sf.channel.close();
        }
    }

    public void closeAll() throws IOException {
        IOException first = null;
        for (SegmentId id : List.copyOf(segmentFiles.keySet())) {
            try {
                close(id);
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private SegmentFile openSegmentFile(SegmentId id) {
        try {
            FileChannel channel = FileChannel.open(pathFor(id),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            // Seed from on-disk size so a restart resumes appending after existing data
            // instead of overwriting it.
            return new SegmentFile(channel, channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path pathFor(SegmentId id) {
        return baseDir.resolve("segment-" + id.value() + ".dat");
    }

    private static final class SegmentFile {
        final FileChannel channel;
        final AtomicLong nextOffset;

        SegmentFile(FileChannel channel, long initialOffset) {
            this.channel = channel;
            this.nextOffset = new AtomicLong(initialOffset);
        }
    }
}
