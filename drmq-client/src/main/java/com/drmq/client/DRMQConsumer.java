package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DRMQ Consumer client for reading messages from topics.
 *
 * Phase 4: Offsets are now stored on the broker per consumer group.
 * On subscribe(), the consumer fetches its last committed offset from the
 * broker and resumes from there. After each poll(), the new offset is
 * automatically committed back to the broker.
 */
public class DRMQConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQConsumer.class);
    private static final int DEFAULT_PORT = 9092;
    private static final int DEFAULT_MAX_MESSAGES = 100;
    private static final String DEFAULT_CONSUMER_GROUP = "default";
    /** Default long-poll wait on the broker side when no messages are available. */
    private static final long DEFAULT_POLL_TIMEOUT_MS = 1000;

    private final String host;
    private final int port;
    private final String consumerGroup;

    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private boolean connected = false;

    // Local cache of current offset per topic (source of truth is the broker)
    private final Map<String, Long> topicOffsets = new HashMap<>();
    private final Object pollLock = new Object();

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public DRMQConsumer() {
        this("localhost", DEFAULT_PORT, DEFAULT_CONSUMER_GROUP);
    }

    public DRMQConsumer(String consumerGroup) {
        this("localhost", DEFAULT_PORT, consumerGroup);
    }

    public DRMQConsumer(String host, int port) {
        this(host, port, DEFAULT_CONSUMER_GROUP);
    }

    public DRMQConsumer(String host, int port, String consumerGroup) {
        this.host = host;
        this.port = port;
        this.consumerGroup = consumerGroup;
    }

    // -------------------------------------------------------------------------
    // Connection
    // -------------------------------------------------------------------------

    public void connect() throws IOException {
        if (connected) {
            return;
        }

        socket = new Socket(host, port);
        inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to DRMQ broker at {}:{} as group '{}'", host, port, consumerGroup);
    }

    // -------------------------------------------------------------------------
    // Subscribe (now fetches offset from broker)
    // -------------------------------------------------------------------------

    /**
     * Subscribe to a topic. Resumes from the broker-committed offset automatically.
     * If no offset has been committed yet, starts from offset 0.
     */
    public void subscribe(String topic) throws IOException {
        if (!connected) connect();
        long offset = fetchOffsetFromBroker(topic);
        topicOffsets.put(topic, offset);
        logger.info("Subscribed to topic '{}' from offset {} (group='{}')", topic, offset, consumerGroup);
    }

    /**
     * Subscribe to a topic, overriding to a specific offset.
     * Useful for replaying or skipping messages.
     */
    public void subscribe(String topic, long fromOffset) throws IOException {
        if (!connected) connect();
        topicOffsets.put(topic, fromOffset);
        logger.info("Subscribed to topic '{}' from explicit offset {} (group='{}')", topic, fromOffset, consumerGroup);
    }

    // -------------------------------------------------------------------------
    // Poll (auto-commits offset after fetching)
    // -------------------------------------------------------------------------

    public List<ConsumedMessage> poll() throws IOException {
        return poll(DEFAULT_MAX_MESSAGES, DEFAULT_POLL_TIMEOUT_MS);
    }

    public List<ConsumedMessage> poll(int maxMessages) throws IOException {
        return poll(maxMessages, DEFAULT_POLL_TIMEOUT_MS);
    }

    /**
     * Poll for messages with a specific max count and long-poll timeout.
     *
     * @param maxMessages maximum number of messages to fetch per topic
     * @param timeoutMs   how long the broker should wait for messages before
     *                    returning empty (0 = return immediately / short poll)
     */
    public List<ConsumedMessage> poll(int maxMessages, long timeoutMs) throws IOException {
        if (!connected) connect();

        synchronized (pollLock) {
            List<ConsumedMessage> allMessages = new ArrayList<>();

            for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
                String topic = entry.getKey();
                long fromOffset = entry.getValue();

                List<ConsumedMessage> messages = fetchMessages(topic, fromOffset, maxMessages, timeoutMs);
                allMessages.addAll(messages);

                if (!messages.isEmpty()) {
                    long nextOffset = messages.get(messages.size() - 1).offset() + 1;
                    topicOffsets.put(topic, nextOffset);
                    commitOffsetToBroker(topic, nextOffset);
                }
            }

            return allMessages;
        }
    }

    // -------------------------------------------------------------------------
    // Manual commit / offset management
    // -------------------------------------------------------------------------

    /**
     * Manually commit a specific offset to the broker.
     */
    public void commit(String topic, long offset) throws IOException {
        if (!connected) connect();
        topicOffsets.put(topic, offset);
        commitOffsetToBroker(topic, offset);
        logger.debug("Manually committed offset {} for topic '{}'", offset, topic);
    }

    public long getCurrentOffset(String topic) {
        return topicOffsets.getOrDefault(topic, 0L);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    // -------------------------------------------------------------------------
    // Broker offset protocol
    // -------------------------------------------------------------------------

    /**
     * Ask the broker for the last committed offset for this group/topic.
     * Returns 0 if no offset has been committed yet.
     */
    private long fetchOffsetFromBroker(String topic) throws IOException {
        FetchOffsetRequest request = FetchOffsetRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setTopic(topic)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.FETCH_OFFSET_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        FetchOffsetResponse response = FetchOffsetResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            logger.warn("Failed to fetch offset from broker for topic '{}': {}", topic, response.getErrorMessage());
            return 0L;
        }

        long offset = response.getOffset();
        // -1 means no committed offset yet → start from 0
        return offset < 0 ? 0L : offset;
    }

    /**
     * Push the current offset for this group/topic to the broker.
     */
    private void commitOffsetToBroker(String topic, long offset) throws IOException {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setTopic(topic)
                .setOffset(offset)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.COMMIT_OFFSET_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        CommitOffsetResponse response = CommitOffsetResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            logger.warn("Failed to commit offset {} for topic '{}': {}", offset, topic, response.getErrorMessage());
        } else {
            logger.debug("Committed offset {} for topic '{}' to broker", offset, topic);
        }
    }

    // -------------------------------------------------------------------------
    // Message fetching
    // -------------------------------------------------------------------------

    private List<ConsumedMessage> fetchMessages(String topic, long fromOffset, int maxMessages) throws IOException {
        return fetchMessages(topic, fromOffset, maxMessages, 0);
    }

    private List<ConsumedMessage> fetchMessages(String topic, long fromOffset, int maxMessages, long timeoutMs) throws IOException {
        ConsumeRequest request = ConsumeRequest.newBuilder()
                .setTopic(topic)
                .setFromOffset(fromOffset)
                .setMaxMessages(maxMessages)
                .setTimeoutMs(timeoutMs)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.CONSUME_REQUEST)
                .setPayload(request.toByteString())
                .build();

        sendEnvelope(envelope);

        MessageEnvelope responseEnvelope = receiveEnvelope();
        ConsumeResponse response = ConsumeResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            throw new IOException("Consume failed: " + response.getErrorMessage());
        }

        List<ConsumedMessage> messages = new ArrayList<>();
        for (StoredMessage msg : response.getMessagesList()) {
            messages.add(new ConsumedMessage(
                    msg.getOffset(),
                    msg.getTopic(),
                    msg.getPayload().toByteArray(),
                    msg.hasKey() ? msg.getKey() : null,
                    msg.getTimestamp(),
                    msg.getStoredAt()
            ));
        }

        logger.debug("Fetched {} messages from topic '{}' starting at offset {}", messages.size(), topic, fromOffset);
        return messages;
    }

    // -------------------------------------------------------------------------
    // Low-level transport helpers
    // -------------------------------------------------------------------------

    private void sendEnvelope(MessageEnvelope envelope) throws IOException {
        byte[] bytes = envelope.toByteArray();
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
        outputStream.flush();
    }

    private MessageEnvelope receiveEnvelope() throws IOException {
        int length = inputStream.readInt();
        byte[] bytes = new byte[length];
        inputStream.readFully(bytes);
        return MessageEnvelope.parseFrom(bytes);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (connected) {
            try {
                if (outputStream != null) outputStream.close();
                if (inputStream != null) inputStream.close();
                if (socket != null) socket.close();
            } finally {
                connected = false;
                logger.info("Disconnected from broker");
            }
        }
    }

    // -------------------------------------------------------------------------
    // ConsumedMessage record
    // -------------------------------------------------------------------------

    public record ConsumedMessage(
            long offset,
            String topic,
            byte[] payload,
            String key,
            long timestamp,
            long storedAt
    ) {
        public String payloadAsString() {
            return new String(payload);
        }
    }
}
