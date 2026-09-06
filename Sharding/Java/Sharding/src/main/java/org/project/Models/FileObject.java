package org.project.Models;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/**
 * Fixed-size 2KB block: 16-byte header + up to 2032 bytes of payload.
 * Fixed size keeps shard-file offset math trivial: fileOffset = blockId * BLOCK_SIZE.
 */
public final class FileObject {

    public static final int BLOCK_SIZE = 2048;
    public static final int HEADER_SIZE = 16;
    public static final int MAX_PAYLOAD_SIZE = BLOCK_SIZE - HEADER_SIZE;

    private static final int MAGIC = 0xFB05CAFE;
    public static final byte CURRENT_VERSION = 1;

    public static final byte FLAG_TOMBSTONE = 1;
    public static final byte FLAG_COMPRESSED = 1 << 1;
    public static final byte FLAG_ENCRYPTED = 1 << 2;

    private final byte version;
    private final byte flags;
    private final byte[] payload;
    private final int checksum;

    public FileObject(byte version, byte flags, byte[] payload) {
        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "payload " + payload.length + " exceeds max " + MAX_PAYLOAD_SIZE);
        }
        this.version = version;
        this.flags = flags;
        this.payload = payload;
        this.checksum = computeChecksum(payload);
    }

    private FileObject(byte version, byte flags, byte[] payload, int checksum) {
        this.version = version;
        this.flags = flags;
        this.payload = payload;
        this.checksum = checksum;
    }

    public static FileObject of(byte[] payload) {
        return new FileObject(CURRENT_VERSION, (byte) 0, payload);
    }

    private static int computeChecksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload);
        return (int) crc.getValue();
    }

    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.put(version);
        buf.put(flags);
        buf.putShort((short) payload.length);
        buf.putInt(checksum);
        buf.putInt(0); // reserved
        buf.put(payload);
        return buf.array(); // remaining bytes stay zero-filled up to BLOCK_SIZE
    }

    public static FileObject fromBytes(byte[] block) {
        if (block.length != BLOCK_SIZE) {
            throw new IllegalArgumentException("block must be exactly " + BLOCK_SIZE + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(block).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IllegalStateException("bad magic, corrupt block");
        }
        byte version = buf.get();
        byte flags = buf.get();
        int length = Short.toUnsignedInt(buf.getShort());
        int checksum = buf.getInt();
        buf.getInt(); // skip reserved

        byte[] payload = new byte[length];
        buf.get(payload);

        if (computeChecksum(payload) != checksum) {
            throw new IllegalStateException("checksum mismatch, corrupt block");
        }
        return new FileObject(version, flags, payload, checksum);
    }

    public boolean isTombstone() {
        return (flags & FLAG_TOMBSTONE) != 0;
    }

    public boolean isCompressed() {
        return (flags & FLAG_COMPRESSED) != 0;
    }

    public byte getVersion() {
        return version;
    }

    public byte getFlags() {
        return flags;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getChecksum() {
        return checksum;
    }

    public int size() {
        return HEADER_SIZE + payload.length;
    }
}
