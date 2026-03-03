package com.example.pulsar;

import org.apache.pulsar.client.api.*;

public class SharedSubscriptionExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/subscription-demo";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Shared subscription - multiple consumers share the load
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName("shared-sub")
                    .subscriptionType(SubscriptionType.Shared)
                    .consumerName("consumer-" + System.currentTimeMillis())
                    .subscribe()) {

                System.out.println("✅ Shared consumer started: " + consumer.getConsumerName());
                System.out.println("📌 Multiple consumers can share this subscription");
                System.out.println("📌 Messages distributed in round-robin fashion\n");

                // Receive messages
                for (int i = 0; i < 10; i++) {
                    Message<byte[]> msg = consumer.receive();
                    System.out.println("[" + consumer.getConsumerName() + "] Received: " + new String(msg.getData()));
                    consumer.acknowledge(msg);
                }

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
