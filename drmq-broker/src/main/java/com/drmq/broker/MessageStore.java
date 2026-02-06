package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
import com.drmq.broker.persistence.LogSegment;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message storage for the broker.
 * Phase 2: Implements persistence using Write-Ahead Logging (WAL) and an in-memory index.
 */
public class MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final AtomicLong globalOffset = new AtomicLong(0);
    private final LogManager logManager;

    // Topic -> Offset -> Byte Position in log file
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> topicIndex = new ConcurrentHashMap<>();
    
    // In-memory cache for recent messages (Topic -> List)
    private final ConcurrentHashMap<String, List<StoredMessage>> messageCache = new ConcurrentHashMap<>();

    public MessageStore(LogManager logManager) {
        this.logManager = logManager;
    }

    /**
     * Recovery: Rebuild the index from log files on disk.
     */
    public void recover() throws IOException {
        logger.info("Starting message store recovery...");
        Map<String, Path> segments = logManager.discoverSegments();
        long maxOffset = -1;

        for (Map.Entry<String, Path> entry : segments.entrySet()) {
            String topic = entry.getKey();
            Path logPath = entry.getValue();
            
            try (LogSegment segment = new LogSegment(logPath)) {
                long position = 0;
                long segmentSize = segment.getSize();
                
                while (position < segmentSize) {
                    StoredMessage message = segment.read(position);
                    long offset = message.getOffset();
                    
                    indexMessage(topic, offset, position);
                    // Also add to cache during recovery for now
                    addToCache(topic, message);
                    
                    if (offset > maxOffset) {
                        maxOffset = offset;
                    }
                    
                    // Move to next message: 4 bytes (length) + message size
                    position += 4 + message.getSerializedSize();
                }
            } catch (Exception e) {
                logger.error("Error recovering topic {}: {}", topic, e.getMessage());
            }
        }

        globalOffset.set(maxOffset + 1);
        logger.info("Recovery complete. Global offset set to {}", globalOffset.get());
    }

    private void indexMessage(String topic, long offset, long position) {
        topicIndex.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .put(offset, position);
    }

    private void addToCache(String topic, StoredMessage message) {
        messageCache.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(message);
    }

    /**
     * Append a message to the specified topic.
     */
    public long append(String topic, byte[] payload, String key, long clientTimestamp) {
        long offset = globalOffset.getAndIncrement();
        long storedAt = System.currentTimeMillis();

        StoredMessage.Builder builder = StoredMessage.newBuilder()
                .setOffset(offset)
                .setTopic(topic)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setTimestamp(clientTimestamp)
                .setStoredAt(storedAt);

        if (key != null && !key.isEmpty()) {
            builder.setKey(key);
        }

        StoredMessage message = builder.build();

        try {
            // 1. Persist to WAL
            LogSegment segment = logManager.getOrCreateSegment(topic);
            long position = segment.append(message);

            // 2. Update Index
            indexMessage(topic, offset, position);

            // 3. Update Cache
            addToCache(topic, message);

            logger.debug("Persisted and indexed message: topic={}, offset={}, position={}", 
                    topic, offset, position);

        } catch (IOException e) {
            logger.error("Failed to persist message for topic {}: {}", topic, e.getMessage());
            throw new RuntimeException("Persistence failure", e);
        }

        return offset;
    }

    /**
     * Get a message by topic and offset.
     */
    public StoredMessage getMessage(String topic, long offset) {
        // First try cache
        List<StoredMessage> messages = messageCache.get(topic);
        if (messages != null) {
            synchronized (messages) {
                for (StoredMessage msg : messages) {
                    if (msg.getOffset() == offset) return msg;
                }
            }
        }

        // Then try index + disk
        Map<Long, Long> index = topicIndex.get(topic);
        if (index != null && index.containsKey(offset)) {
            try {
                long position = index.get(offset);
                LogSegment segment = logManager.getOrCreateSegment(topic);
                return segment.read(position);
            } catch (IOException e) {
                logger.error("Error reading message from disk: topic={}, offset={}", topic, offset, e);
            }
        }

        return null;
    }

    /**
     * Get messages from a topic starting at the given offset.
     */
    public List<StoredMessage> getMessages(String topic, long fromOffset, int maxCount) {
        // For Phase 2, we'll leverage the cache if possible, or read from disk sequentially.
        // Keeping it simple: filter the cache for now, as recovery loads everything into cache.
        List<StoredMessage> messages = messageCache.get(topic);
        if (messages == null) {
            return Collections.emptyList();
        }

        List<StoredMessage> result = new ArrayList<>();
        synchronized (messages) {
            for (StoredMessage msg : messages) {
                if (msg.getOffset() >= fromOffset) {
                    result.add(msg);
                    if (result.size() >= maxCount) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    public long getCurrentOffset() {
        return globalOffset.get();
    }

    public int getMessageCount(String topic) {
        Map<Long, Long> index = topicIndex.get(topic);
        return index == null ? 0 : index.size();
    }

    public List<String> getTopics() {
        return new ArrayList<>(topicIndex.keySet());
    }

    public void clear() {
        topicIndex.clear();
        messageCache.clear();
        globalOffset.set(0);
        // Note: This doesn't delete files from disk for safety, but clears memory view.
        logger.info("Message store memory state cleared");
    }
}

