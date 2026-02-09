package com.drmq.broker;

import com.drmq.protocol.DRMQProtocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Handles a single client connection to the broker.
 * Reads length-prefixed protobuf messages, processes them, and sends responses.
 */
public class ClientHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket socket;
    private final MessageStore messageStore;
    private volatile boolean running = true;

    public ClientHandler(Socket socket, MessageStore messageStore) {
        this.socket = socket;
        this.messageStore = messageStore;
    }

    @Override
    public void run() {
        String clientAddress = socket.getRemoteSocketAddress().toString();
        logger.info("Client connected: {}", clientAddress);

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running && !socket.isClosed()) {
                try {
                    // Read message length (4 bytes, big-endian)
                    int length = in.readInt();
                    if (length <= 0 || length > 10 * 1024 * 1024) { // Max 10MB message
                        logger.warn("Invalid message length: {}", length);
                        break;
                    }

                    // Read the envelope
                    byte[] envelopeBytes = new byte[length];
                    in.readFully(envelopeBytes);

                    MessageEnvelope envelope = MessageEnvelope.parseFrom(envelopeBytes);
                    MessageEnvelope response = handleMessage(envelope);

                    // Send response
                    byte[] responseBytes = response.toByteArray();
                    out.writeInt(responseBytes.length);
                    out.write(responseBytes);
                    out.flush();

                } catch (EOFException e) {
                    // Client disconnected normally
                    logger.info("Client disconnected: {}", clientAddress);
                    break;
                }
            }

        } catch (IOException e) {
            if (running) {
                logger.error("Error handling client {}: {}", clientAddress, e.getMessage());
            }
        } finally {
            closeSocket();
        }
    }

    /**
     * Dispatch incoming message to appropriate handler based on type.
     */
    private MessageEnvelope handleMessage(MessageEnvelope envelope) throws IOException {
        return switch (envelope.getType()) {
            case PRODUCE_REQUEST -> handleProduceRequest(envelope);
            case CONSUME_REQUEST -> handleConsumeRequest(envelope);
            default -> createErrorResponse("Unknown message type: " + envelope.getType());
        };
    }

    /**
     * Handle a produce request - store the message and return the assigned offset.
     */
    private MessageEnvelope handleProduceRequest(MessageEnvelope envelope) throws IOException {
        try {
            ProduceRequest request = ProduceRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            byte[] payload = request.getPayload().toByteArray();
            String key = request.hasKey() ? request.getKey() : null;
            long timestamp = request.getTimestamp();

            // Store the message
            long offset = messageStore.append(topic, payload, key, timestamp);

            logger.debug("Produced message: topic={}, offset={}", topic, offset);

            // Build success response
            ProduceResponse response = ProduceResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(offset)
                    .build();

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.PRODUCE_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing produce request", e);
            return createProduceErrorResponse(e.getMessage());
        }
    }

    /**
     * Handle a consume request - fetch messages from the specified offset.
     */
    private MessageEnvelope handleConsumeRequest(MessageEnvelope envelope) throws IOException {
        try {
            ConsumeRequest request = ConsumeRequest.parseFrom(envelope.getPayload());

            String topic = request.getTopic();
            long fromOffset = request.getFromOffset();
            int maxMessages = request.getMaxMessages();

            // Fetch messages from store
            var messages = messageStore.getMessages(topic, fromOffset, maxMessages);

            logger.debug("Consumed {} messages: topic={}, fromOffset={}", 
                    messages.size(), topic, fromOffset);

            // Build success response
            ConsumeResponse response = ConsumeResponse.newBuilder()
                    .setSuccess(true)
                    .addAllMessages(messages)
                    .build();

            return MessageEnvelope.newBuilder()
                    .setType(MessageType.CONSUME_RESPONSE)
                    .setPayload(response.toByteString())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing consume request", e);
            return createConsumeErrorResponse(e.getMessage());
        }
    }

    /**
     * Create an error response envelope for produce requests.
     */
    private MessageEnvelope createProduceErrorResponse(String errorMessage) {
        ProduceResponse response = ProduceResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.PRODUCE_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    /**
     * Create an error response envelope for consume requests.
     */
    private MessageEnvelope createConsumeErrorResponse(String errorMessage) {
        ConsumeResponse response = ConsumeResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(errorMessage != null ? errorMessage : "Unknown error")
                .build();

        return MessageEnvelope.newBuilder()
                .setType(MessageType.CONSUME_RESPONSE)
                .setPayload(response.toByteString())
                .build();
    }

    /**
     * Create a generic error response envelope (deprecated - use specific error methods).
     */
    @Deprecated
    private MessageEnvelope createErrorResponse(String errorMessage) {
        return createProduceErrorResponse(errorMessage);
    }

    /**
     * Stop processing and close the connection.
     */
    public void stop() {
        running = false;
        closeSocket();
    }

    private void closeSocket() {
        try {
            if (!socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing socket", e);
        }
    }
}
