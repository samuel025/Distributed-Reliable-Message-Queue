package com.drmq.client;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * DRMQ Producer client for sending messages to the broker.
 * 
 * Thread-safe: can be used by multiple threads to send messages concurrently.
 * Uses synchronous send with response waiting.
 */
public class DRMQProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DRMQProducer.class);

    private final String host;
    private final int port;
    private final Object sendLock = new Object();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private volatile boolean connected = false;

    /**
     * Create a producer targeting the specified broker.
     */
    public DRMQProducer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Create a producer targeting localhost with default port.
     */
    public DRMQProducer() {
        this("localhost", 9092);
    }

    /**
     * Connect to the broker.
     */
    public void connect() throws IOException {
        if (connected) {
            return;
        }

        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        connected = true;

        logger.info("Connected to broker at {}:{}", host, port);
    }

    /**
     * Send a message to the specified topic.
     *
     * @param topic   The topic name
     * @param payload The message payload
     * @return The result containing the assigned offset or error
     */
    public SendResult send(String topic, byte[] payload) throws IOException {
        return send(topic, payload, null);
    }

    /**
     * Send a string message to the specified topic.
     */
    public SendResult send(String topic, String message) throws IOException {
        return send(topic, message.getBytes(StandardCharsets.UTF_8), null);
    }

    /**
     * Send a message with an optional key to the specified topic.
     *
     * @param topic   The topic name
     * @param payload The message payload
     * @param key     Optional message key (can be null)
     * @return The result containing the assigned offset or error
     */
    public SendResult send(String topic, byte[] payload, String key) throws IOException {
        if (!connected) {
            connect();
        }

        // Build the produce request
        ProduceRequest.Builder requestBuilder = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setTimestamp(System.currentTimeMillis());

        if (key != null && !key.isEmpty()) {
            requestBuilder.setKey(key);
        }

        ProduceRequest request = requestBuilder.build();

        // Wrap in envelope
        MessageEnvelope envelope = MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_REQUEST)
                .setPayload(request.toByteString())
                .build();

        // Send and receive (synchronized for thread safety)
        synchronized (sendLock) {
            byte[] envelopeBytes = envelope.toByteArray();
            out.writeInt(envelopeBytes.length);
            out.write(envelopeBytes);
            out.flush();

            // Read response
            int responseLength = in.readInt();
            byte[] responseBytes = new byte[responseLength];
            in.readFully(responseBytes);

            MessageEnvelope responseEnvelope = MessageEnvelope.parseFrom(responseBytes);
            ProduceResponse response = ProduceResponse.parseFrom(responseEnvelope.getPayload());

            if (response.getSuccess()) {
                logger.debug("Message sent: topic={}, offset={}", topic, response.getOffset());
                return SendResult.success(response.getOffset());
            } else {
                logger.warn("Message send failed: {}", response.getErrorMessage());
                return SendResult.failure(response.getErrorMessage());
            }
        }
    }

    /**
     * Check if connected to the broker.
     */
    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
    }

    /**
     * Close the connection to the broker.
     */
    @Override
    public void close() {
        connected = false;
        
        try {
            if (in != null) in.close();
        } catch (IOException e) {
            logger.debug("Error closing input stream", e);
        }
        
        try {
            if (out != null) out.close();
        } catch (IOException e) {
            logger.debug("Error closing output stream", e);
        }
        
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing socket", e);
        }

        logger.info("Disconnected from broker");
    }

    /**
     * Result of a send operation.
     */
    public static class SendResult {
        private final boolean success;
        private final long offset;
        private final String errorMessage;

        private SendResult(boolean success, long offset, String errorMessage) {
            this.success = success;
            this.offset = offset;
            this.errorMessage = errorMessage;
        }

        public static SendResult success(long offset) {
            return new SendResult(true, offset, null);
        }

        public static SendResult failure(String errorMessage) {
            return new SendResult(false, -1, errorMessage);
        }

        public boolean isSuccess() {
            return success;
        }

        public long getOffset() {
            return offset;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            if (success) {
                return "SendResult{success=true, offset=" + offset + "}";
            } else {
                return "SendResult{success=false, error='" + errorMessage + "'}";
            }
        }
    }
}
