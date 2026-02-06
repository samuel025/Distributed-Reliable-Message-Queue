package com.drmq.broker;

import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory message storage for the broker.
 * Thread-safe storage of messages organized by topic with monotonically increasing offsets.
 * 
 * This is the Phase 1 implementation - messages are stored in memory only.
 * Phase 2 will add write-ahead logging for persistence.
 */
public class MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    // Global offset counter - monotonically increasing across all topics
    private final AtomicLong globalOffset = new AtomicLong(0);

    // Topic -> List of messages mapping
    private final ConcurrentHashMap<String, List<StoredMessage>> topicMessages = new ConcurrentHashMap<>();

    /**
     * Append a message to the specified topic.
     *
     * @param topic   The topic name
     * @param payload The message payload bytes
     * @param key     Optional message key (may be null)
     * @param clientTimestamp The client-provided timestamp
     * @return The assigned offset for this message
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

        // Get or create the message list for this topic
        List<StoredMessage> messages = topicMessages.computeIfAbsent(topic, 
                k -> Collections.synchronizedList(new ArrayList<>()));
        messages.add(message);

        logger.debug("Stored message: topic={}, offset={}, size={} bytes", 
                topic, offset, payload.length);

        return offset;
    }

    /**
     * Get a message by topic and offset.
     *
     * @param topic  The topic name
     * @param offset The message offset
     * @return The stored message, or null if not found
     */
    public StoredMessage getMessage(String topic, long offset) {
        List<StoredMessage> messages = topicMessages.get(topic);
        if (messages == null) {
            return null;
        }

        // Linear search - acceptable for Phase 1, will be optimized with index in Phase 2
        synchronized (messages) {
            for (StoredMessage msg : messages) {
                if (msg.getOffset() == offset) {
                    return msg;
                }
            }
        }
        return null;
    }

    /**
     * Get messages from a topic starting at the given offset.
     *
     * @param topic     The topic name
     * @param fromOffset Starting offset (inclusive)
     * @param maxCount  Maximum number of messages to return
     * @return List of messages, may be empty
     */
    public List<StoredMessage> getMessages(String topic, long fromOffset, int maxCount) {
        List<StoredMessage> messages = topicMessages.get(topic);
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

    /**
     * Get the current global offset (next offset to be assigned).
     */
    public long getCurrentOffset() {
        return globalOffset.get();
    }

    /**
     * Get the number of messages in a topic.
     */
    public int getMessageCount(String topic) {
        List<StoredMessage> messages = topicMessages.get(topic);
        return messages == null ? 0 : messages.size();
    }

    /**
     * Get all topic names.
     */
    public List<String> getTopics() {
        return new ArrayList<>(topicMessages.keySet());
    }

    /**
     * Clear all messages (for testing purposes).
     */
    public void clear() {
        topicMessages.clear();
        globalOffset.set(0);
        logger.info("Message store cleared");
    }
}
