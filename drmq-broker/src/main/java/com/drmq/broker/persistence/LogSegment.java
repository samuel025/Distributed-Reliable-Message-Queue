package com.drmq.broker.persistence;

import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Manages a single log file for a topic.
 * Uses FileChannel for efficient I/O and ensures durability with fsync.
 */
public class LogSegment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final Path filePath;
    private final FileChannel fileChannel;
    private long currentSize;

    public LogSegment(Path filePath) throws IOException {
        this.filePath = filePath;
        this.fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.currentSize = fileChannel.size();
        logger.info("Opened log segment: {} (size: {} bytes)", this.filePath, currentSize);
    }

    /**
     * Append a message to the log segment.
     * @param message The message to append.
     * @return The starting position of the message in the file.
     * @throws IOException If a write error occurs.
     */
    public synchronized long append(StoredMessage message) throws IOException {
        byte[] messageBytes = message.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(4 + messageBytes.length);
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);
        buffer.flip();

        long position = currentSize;
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer, position + buffer.position());
        }
        
        // Ensure durability
        fileChannel.force(true);
        
        currentSize += buffer.limit();
        return position;
    }

    /**
     * Read a message from the specified position.
     * @param position The position to read from.
     * @return The stored message.
     * @throws IOException If a read error occurs.
     */
    public StoredMessage read(long position) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        fileChannel.read(lengthBuffer, position);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        ByteBuffer messageBuffer = ByteBuffer.allocate(length);
        fileChannel.read(messageBuffer, position + 4);
        messageBuffer.flip();

        return StoredMessage.parseFrom(messageBuffer.array());
    }

    public long getSize() {
        return currentSize;
    }

    @Override
    public void close() throws IOException {
        if (fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}
