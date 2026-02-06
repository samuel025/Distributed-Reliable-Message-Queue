package com.drmq.broker.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Manages all log segments across different topics.
 * Handles directory structure and recovery on startup.
 */
public class LogManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);
    private static final String DEFAULT_DATA_DIR = "./data";
    private static final String LOG_FILE_SUFFIX = ".log";

    private final Path dataDir;
    private final Map<String, LogSegment> topicSegments = new ConcurrentHashMap<>();

    public LogManager(String dataDirStr) throws IOException {
        this.dataDir = Paths.get(dataDirStr != null ? dataDirStr : DEFAULT_DATA_DIR);
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        logger.info("LogManager initialized with data directory: {}", dataDir.toAbsolutePath());
    }

    public LogManager() throws IOException {
        this(DEFAULT_DATA_DIR);
    }

    /**
     * Get or create a log segment for a topic.
     */
    public LogSegment getOrCreateSegment(String topic) throws IOException {
        return topicSegments.computeIfAbsent(topic, t -> {
            try {
                Path topicDir = dataDir.resolve(t);
                if (!Files.exists(topicDir)) {
                    Files.createDirectories(topicDir);
                }
                // For Phase 2, we use a single segment "00000000.log" per topic
                Path logPath = topicDir.resolve("00000000" + LOG_FILE_SUFFIX);
                return new LogSegment(logPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create log segment for topic: " + t, e);
            }
        });
    }

    /**
     * Recovery: Scan the data directory and load existing segments.
     * This allows rebuild of memory index by the caller.
     */
    public Map<String, Path> discoverSegments() throws IOException {
        Map<String, Path> segments = new ConcurrentHashMap<>();
        if (!Files.exists(dataDir)) return segments;

        try (Stream<Path> topicDirs = Files.list(dataDir)) {
            topicDirs.filter(Files::isDirectory).forEach(topicDir -> {
                String topic = topicDir.getFileName().toString();
                Path logPath = topicDir.resolve("00000000" + LOG_FILE_SUFFIX);
                if (Files.exists(logPath)) {
                    segments.put(topic, logPath);
                }
            });
        }
        return segments;
    }

    @Override
    public void close() throws IOException {
        for (LogSegment segment : topicSegments.values()) {
            segment.close();
        }
        topicSegments.clear();
    }
}
