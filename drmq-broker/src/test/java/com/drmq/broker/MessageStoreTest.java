package com.drmq.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MessageStore.
 */
class MessageStoreTest {

    private MessageStore store;

    @BeforeEach
    void setUp() {
        store = new MessageStore();
    }

    @Test
    void appendReturnsMonotonicallyIncreasingOffsets() {
        long offset1 = store.append("test-topic", "message1".getBytes(), null, System.currentTimeMillis());
        long offset2 = store.append("test-topic", "message2".getBytes(), null, System.currentTimeMillis());
        long offset3 = store.append("test-topic", "message3".getBytes(), null, System.currentTimeMillis());

        assertEquals(0, offset1);
        assertEquals(1, offset2);
        assertEquals(2, offset3);
    }

    @Test
    void offsetsAreGlobalAcrossTopics() {
        long offset1 = store.append("topic-a", "msg1".getBytes(), null, System.currentTimeMillis());
        long offset2 = store.append("topic-b", "msg2".getBytes(), null, System.currentTimeMillis());
        long offset3 = store.append("topic-a", "msg3".getBytes(), null, System.currentTimeMillis());

        assertEquals(0, offset1);
        assertEquals(1, offset2);
        assertEquals(2, offset3);
    }

    @Test
    void getMessageReturnsStoredMessage() {
        byte[] payload = "hello world".getBytes();
        long offset = store.append("test", payload, "key1", 12345L);

        var message = store.getMessage("test", offset);

        assertNotNull(message);
        assertEquals(offset, message.getOffset());
        assertEquals("test", message.getTopic());
        assertArrayEquals(payload, message.getPayload().toByteArray());
        assertEquals("key1", message.getKey());
        assertEquals(12345L, message.getTimestamp());
    }

    @Test
    void getMessageReturnsNullForNonexistentOffset() {
        store.append("test", "msg".getBytes(), null, System.currentTimeMillis());

        assertNull(store.getMessage("test", 999));
        assertNull(store.getMessage("nonexistent", 0));
    }

    @Test
    void getMessagesReturnsRangeFromOffset() {
        for (int i = 0; i < 10; i++) {
            store.append("test", ("msg" + i).getBytes(), null, System.currentTimeMillis());
        }

        var messages = store.getMessages("test", 5, 3);

        assertEquals(3, messages.size());
        assertEquals(5, messages.get(0).getOffset());
        assertEquals(6, messages.get(1).getOffset());
        assertEquals(7, messages.get(2).getOffset());
    }

    @Test
    void concurrentAppendsProduceUniqueOffsets() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger totalMessages = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadNum = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerThread; i++) {
                        store.append("topic-" + threadNum, 
                                ("message-" + i).getBytes(), null, System.currentTimeMillis());
                        totalMessages.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify all messages were stored with unique offsets
        assertEquals(threadCount * messagesPerThread, store.getCurrentOffset());
        assertEquals(threadCount * messagesPerThread, totalMessages.get());
    }

    @Test
    void clearResetsStore() {
        store.append("test", "msg".getBytes(), null, System.currentTimeMillis());
        assertEquals(1, store.getCurrentOffset());
        assertEquals(1, store.getMessageCount("test"));

        store.clear();

        assertEquals(0, store.getCurrentOffset());
        assertEquals(0, store.getMessageCount("test"));
    }

    @Test
    void getTopicsReturnsAllTopicNames() {
        store.append("alpha", "msg".getBytes(), null, System.currentTimeMillis());
        store.append("beta", "msg".getBytes(), null, System.currentTimeMillis());
        store.append("gamma", "msg".getBytes(), null, System.currentTimeMillis());

        var topics = store.getTopics();

        assertEquals(3, topics.size());
        assertTrue(topics.contains("alpha"));
        assertTrue(topics.contains("beta"));
        assertTrue(topics.contains("gamma"));
    }
}
