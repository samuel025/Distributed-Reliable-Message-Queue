package com.drmq.broker;

import com.drmq.broker.persistence.LogManager;
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
 */
public class BrokerServer {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    public static final String DEFAULT_DATA_DIR = "./data";

    private final int port;
    private final String dataDir;
    private final ExecutorService executor;
    private final MessageStore messageStore;
    private final LogManager logManager;
    private final List<ClientHandler> activeHandlers = new ArrayList<>();

    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public BrokerServer(int port, int threadPoolSize, String dataDir) throws IOException {
        this.port = port;
        this.dataDir = dataDir;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
        this.logManager = new LogManager(dataDir);
        this.messageStore = new MessageStore(logManager);
    }

    public BrokerServer(int port, int threadPoolSize) throws IOException {
        this(port, threadPoolSize, DEFAULT_DATA_DIR);
    }

    public BrokerServer() throws IOException {
        this(DEFAULT_PORT, DEFAULT_THREAD_POOL_SIZE);
    }

    /**
     * Start the broker server. Blocks until shutdown.
     */
    public void start() throws IOException {
        // Recovery phase
        try {
            messageStore.recover();
        } catch (IOException e) {
            logger.error("Failed to recover message store: {}", e.getMessage());
            throw e;
        }

        serverSocket = new ServerSocket(port);
        running = true;

        logger.info("DRMQ Broker started on port {} with data directory {}", port, dataDir);

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

        // Close LogManager
        try {
            if (logManager != null) {
                logManager.close();
            }
        } catch (IOException e) {
            logger.error("Error closing log manager", e);
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

    public boolean isRunning() {
        return running;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public int getPort() {
        return port;
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        String dataDir = DEFAULT_DATA_DIR;
        
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.exit(1);
            }
        }
        
        if (args.length > 1) {
            dataDir = args[1];
        }

        try {
            BrokerServer broker = new BrokerServer(port, DEFAULT_THREAD_POOL_SIZE, dataDir);
            
            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown));
            
            broker.start();
        } catch (IOException e) {
            logger.error("Failed to start broker", e);
            System.exit(1);
        }
    }
}

