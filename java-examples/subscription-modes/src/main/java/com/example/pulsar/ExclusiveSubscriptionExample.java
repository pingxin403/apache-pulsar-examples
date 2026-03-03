package com.example.pulsar;

import org.apache.pulsar.client.api.*;

public class ExclusiveSubscriptionExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/subscription-demo";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Exclusive subscription - only one consumer can subscribe
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName("exclusive-sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe()) {

                System.out.println("✅ Exclusive consumer started");
                System.out.println("📌 Only ONE consumer can use this subscription\n");

                // Receive messages
                for (int i = 0; i < 5; i++) {
                    Message<byte[]> msg = consumer.receive();
                    System.out.println("Received: " + new String(msg.getData()));
                    consumer.acknowledge(msg);
                }

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
