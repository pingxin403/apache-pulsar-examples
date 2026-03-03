package com.example.pulsar;

import org.apache.pulsar.client.api.*;

public class KeySharedSubscriptionExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/subscription-demo";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Key_Shared subscription - parallel processing with ordering guarantee
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName("key-shared-sub")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .consumerName("consumer-" + System.currentTimeMillis())
                    .subscribe()) {

                System.out.println("✅ Key_Shared consumer started: " + consumer.getConsumerName());
                System.out.println("📌 Multiple consumers process in parallel");
                System.out.println("📌 Same key always goes to same consumer");
                System.out.println("📌 Ordering guaranteed per key\n");

                // Receive messages
                for (int i = 0; i < 10; i++) {
                    Message<byte[]> msg = consumer.receive();
                    String key = msg.getKey() != null ? msg.getKey() : "no-key";
                    System.out.println("[" + consumer.getConsumerName() + "] Key: " + key + ", Message: " + new String(msg.getData()));
                    consumer.acknowledge(msg);
                }

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
