package com.drmq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * DRMQ Broker Server - TCP server accepting producer connections.
 * 
 * Uses a thread pool to handle concurrent client connections.
 * Each connection is managed by a ClientHandler.
 */
public class BrokerServer {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private final int port;
    private final ExecutorService executor;
    private final MessageStore messageStore;
    private final List<ClientHandler> activeHandlers = new ArrayList<>();

    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public BrokerServer(int port, int threadPoolSize) {
        this.port = port;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
        this.messageStore = new MessageStore();
    }

    public BrokerServer() {
        this(DEFAULT_PORT, DEFAULT_THREAD_POOL_SIZE);
    }

    /**
     * Start the broker server. Blocks until shutdown.
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        logger.info("DRMQ Broker started on port {}", port);

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                ClientHandler handler = new ClientHandler(clientSocket, messageStore);
                
                synchronized (activeHandlers) {
                    activeHandlers.add(handler);
                }
                
                executor.submit(handler);
            } catch (IOException e) {
                if (running) {
                    logger.error("Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Start the broker in a background thread.
     */
    public void startAsync() {
        Thread serverThread = new Thread(() -> {
            try {
                start();
            } catch (IOException e) {
                logger.error("Broker server error", e);
            }
        }, "broker-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to be ready
        while (!running && serverThread.isAlive()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Gracefully shutdown the broker.
     */
    public void shutdown() {
        logger.info("Shutting down broker...");
        running = false;

        // Close server socket to stop accepting new connections
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing server socket", e);
        }

        // Stop all active handlers
        synchronized (activeHandlers) {
            for (ClientHandler handler : activeHandlers) {
                handler.stop();
            }
            activeHandlers.clear();
        }

        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Broker shutdown complete");
    }

    /**
     * Check if the broker is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Get the message store (for testing).
     */
    public MessageStore getMessageStore() {
        return messageStore;
    }

    /**
     * Get the port the broker is listening on.
     */
    public int getPort() {
        return port;
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.exit(1);
            }
        }

        BrokerServer broker = new BrokerServer(port, DEFAULT_THREAD_POOL_SIZE);

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown));

        try {
            broker.start();
        } catch (IOException e) {
            logger.error("Failed to start broker", e);
            System.exit(1);
        }
    }
}
