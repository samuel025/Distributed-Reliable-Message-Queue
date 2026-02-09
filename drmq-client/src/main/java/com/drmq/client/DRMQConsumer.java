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
 * Thread-safe for single-threaded use (one poll at a time).
 */
public class DRMQConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQConsumer.class);
    private static final int DEFAULT_PORT = 9092;
    private static final int DEFAULT_MAX_MESSAGES = 100;

    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private boolean connected = false;

    // Track current offset per topic
    private final Map<String, Long> topicOffsets = new HashMap<>();
    private final Object pollLock = new Object();

    public DRMQConsumer() {
        this("localhost", DEFAULT_PORT);
    }

    public DRMQConsumer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connect to the broker.
     */
    public void connect() throws IOException {
        if (connected) {
            return;
        }

        socket = new Socket(host, port);
        inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to DRMQ broker at {}:{}", host, port);
    }

    /**
     * Subscribe to a topic starting from offset 0.
     */
    public void subscribe(String topic) {
        subscribe(topic, 0);
    }

    /**
     * Subscribe to a topic starting from a specific offset.
     */
    public void subscribe(String topic, long fromOffset) {
        topicOffsets.put(topic, fromOffset);
        logger.info("Subscribed to topic '{}' from offset {}", topic, fromOffset);
    }

    /**
     * Poll for messages from all subscribed topics.
     * Returns messages and updates internal offset tracking.
     */
    public List<ConsumedMessage> poll() throws IOException {
        return poll(DEFAULT_MAX_MESSAGES);
    }

    /**
     * Poll for messages with a specific max count.
     */
    public List<ConsumedMessage> poll(int maxMessages) throws IOException {
        if (!connected) {
            connect();
        }

        synchronized (pollLock) {
            List<ConsumedMessage> allMessages = new ArrayList<>();

            for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
                String topic = entry.getKey();
                long fromOffset = entry.getValue();

                List<ConsumedMessage> messages = fetchMessages(topic, fromOffset, maxMessages);
                allMessages.addAll(messages);

                // Update offset to the next message after the last one fetched
                if (!messages.isEmpty()) {
                    long lastOffset = messages.get(messages.size() - 1).offset();
                    topicOffsets.put(topic, lastOffset + 1);
                }
            }

            return allMessages;
        }
    }

    /**
     * Fetch messages from a specific topic and offset.
     */
    private List<ConsumedMessage> fetchMessages(String topic, long fromOffset, int maxMessages) throws IOException {
        // Build consume request
        ConsumeRequest request = ConsumeRequest.newBuilder()
                .setTopic(topic)
                .setFromOffset(fromOffset)
                .setMaxMessages(maxMessages)
                .build();

        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.CONSUME_REQUEST)
                .setPayload(request.toByteString())
                .build();

        // Send request
        byte[] envelopeBytes = envelope.toByteArray();
        outputStream.writeInt(envelopeBytes.length);
        outputStream.write(envelopeBytes);
        outputStream.flush();

        // Read response
        int responseLength = inputStream.readInt();
        byte[] responseBytes = new byte[responseLength];
        inputStream.readFully(responseBytes);

        MessageEnvelope responseEnvelope = MessageEnvelope.parseFrom(responseBytes);
        ConsumeResponse response = ConsumeResponse.parseFrom(responseEnvelope.getPayload());

        if (!response.getSuccess()) {
            throw new IOException("Consume failed: " + response.getErrorMessage());
        }

        // Convert to ConsumedMessage records
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

        logger.debug("Fetched {} messages from topic '{}' starting at offset {}", 
                messages.size(), topic, fromOffset);

        return messages;
    }

    /**
     * Manually commit the current offset for a topic.
     */
    public void commit(String topic, long offset) {
        topicOffsets.put(topic, offset);
        logger.debug("Committed offset {} for topic '{}'", offset, topic);
    }

    /**
     * Get the current offset for a topic.
     */
    public long getCurrentOffset(String topic) {
        return topicOffsets.getOrDefault(topic, 0L);
    }

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

    /**
     * Record representing a consumed message.
     */
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
