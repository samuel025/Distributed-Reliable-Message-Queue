package com.drmq.client;

import java.io.IOException;

/**
 * A simple manual test application to verify DRMQ Phase 1.
 */
public class ManualTestApp {
    public static void main(String[] args) {
        System.out.println("--- DRMQ Manual Test Start ---");
        
        // 1. Create the producer (default connects to localhost:9092)
        try (DRMQProducer producer = new DRMQProducer()) {
            
            // 2. Connect to the broker
            System.out.println("Connecting to broker...");
            producer.connect();
            System.out.println("Connected!");

            // 3. Send a few different messages
            send(producer, "orders", "Pizza Order #101");
            send(producer, "orders", "Burger Order #102");
            send(producer, "payments", "Payment of $25.50 for Order #101");
            send(producer, "alerts", "System is ONLINE");

            System.out.println("\n--- All messages sent successfully! ---");
            
        } catch (IOException e) {
            System.err.println("FAILED to connect or send: " + e.getMessage());
            System.err.println("\nIs the Broker running? Run 'mvn exec:java' in the drmq-broker directory first!");
        }
    }

    private static void send(DRMQProducer producer, String topic, String content) throws IOException {
        System.out.printf("Sending to [%s]: %s... ", topic, content);
        DRMQProducer.SendResult result = producer.send(topic, content);
        
        if (result.isSuccess()) {
            System.out.println("SUCCESS! (Offset: " + result.getOffset() + ")");
        } else {
            System.out.println("FAILED: " + result.getErrorMessage());
        }
    }
}
