package com.drmq.client.commandLineExample;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import com.drmq.client.DRMQConsumer;

/**
 * Interactive Consumer CLI - allows subscribing and polling via command line.
 * Subscribe to topics and poll for messages interactively.
 */
public class ConsumerApp {
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║     DRMQ Interactive Consumer CLI     ║");
        System.out.println("╚════════════════════════════════════════╝\n");
        
        try (DRMQConsumer consumer = new DRMQConsumer();
             Scanner scanner = new Scanner(System.in)) {
            
            consumer.connect();
            System.out.println("✓ Connected to broker at localhost:9092\n");
            System.out.println("Commands:");
            System.out.println("  subscribe <topic>       - Subscribe to a topic");
            System.out.println("  subscribe <topic> <offset> - Subscribe from specific offset");
            System.out.println("  poll [max_messages]     - Poll for messages (default: 100)");
            System.out.println("  status                  - Show subscribed topics and offsets");
            System.out.println("  exit                    - Quit the application");
            System.out.println("  help                    - Show this help\n");
            System.out.println("Examples:");
            System.out.println("  subscribe orders");
            System.out.println("  subscribe payments 5");
            System.out.println("  poll");
            System.out.println("  poll 10\n");
            System.out.println("─────────────────────────────────────────\n");

            // Interactive loop
            while (true) {
                System.out.print("consumer> ");
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) {
                    continue;
                }
                
                String[] parts = input.split("\\s+");
                String command = parts[0].toLowerCase();
                
                switch (command) {
                    case "exit", "quit" -> {
                        System.out.println("\n✓ Goodbye!");
                        return;
                    }
                    
                    case "help" -> {
                        System.out.println("\nCommands:");
                        System.out.println("  subscribe <topic> [offset] - Subscribe to topic");
                        System.out.println("  poll [max_messages]        - Poll for messages");
                        System.out.println("  exit                       - Quit");
                        System.out.println("  help                       - Show help\n");
                    }
                    
                    case "subscribe", "sub" -> {
                        if (parts.length < 2) {
                            System.out.println("❌ Usage: subscribe <topic> [offset]");
                            System.out.println("   Example: subscribe orders");
                            System.out.println("   Example: subscribe payments 5\n");
                            continue;
                        }
                        
                        String topic = parts[1];
                        long offset = 0;
                        
                        if (parts.length >= 3) {
                            try {
                                offset = Long.parseLong(parts[2]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid offset: " + parts[2] + "\n");
                                continue;
                            }
                        }
                        
                        consumer.subscribe(topic, offset);
                        System.out.printf("✓ Subscribed to [%s] from offset %d\n\n", topic, offset);
                    }
                    
                    case "poll" -> {
                        int maxMessages = 100;
                        
                        if (parts.length >= 2) {
                            try {
                                maxMessages = Integer.parseInt(parts[1]);
                            } catch (NumberFormatException e) {
                                System.out.println("❌ Invalid max_messages: " + parts[1] + "\n");
                                continue;
                            }
                        }
                        
                        try {
                            System.out.println("Polling for messages...");
                            List<DRMQConsumer.ConsumedMessage> messages = consumer.poll(maxMessages);
                            
                            if (messages.isEmpty()) {
                                System.out.println("  No new messages\n");
                            } else {
                                System.out.printf("  Received %d message(s):\n\n", messages.size());
                                
                                for (DRMQConsumer.ConsumedMessage msg : messages) {
                                    System.out.printf("  ┌─ [%s] Offset: %d\n", msg.topic(), msg.offset());
                                    System.out.printf("  │  Message: %s\n", msg.payloadAsString());
                                    if (msg.key() != null) {
                                        System.out.printf("  │  Key: %s\n", msg.key());
                                    }
                                    System.out.printf("  │  Timestamp: %d\n", msg.timestamp());
                                    System.out.println("  └─");
                                }
                                System.out.println();
                            }
                        } catch (IOException e) {
                            System.out.printf("❌ Error polling: %s\n\n", e.getMessage());
                        }
                    }
                    
                    
                    default -> {
                        System.out.printf("❌ Unknown command: %s\n", command);
                        System.out.println("   Type 'help' for available commands\n");
                    }
                }
            }
            
        } catch (IOException e) {
            System.err.println("\n❌ Failed to connect to broker: " + e.getMessage());
            System.err.println("\nMake sure the broker is running:");
            System.err.println("  cd drmq-broker && mvn exec:java\n");
        }
    }
}
