package com.example.pulsar;

import org.apache.pulsar.client.api.*;

public class FailoverSubscriptionExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/subscription-demo";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Failover subscription - one active consumer, others standby
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName("failover-sub")
                    .subscriptionType(SubscriptionType.Failover)
                    .consumerName("consumer-" + System.currentTimeMillis())
                    .subscribe()) {

                System.out.println("✅ Failover consumer started: " + consumer.getConsumerName());
                System.out.println("📌 One ACTIVE consumer, others are STANDBY");
                System.out.println("📌 Automatic failover if active consumer fails\n");

                // Receive messages
                for (int i = 0; i < 10; i++) {
                    Message<byte[]> msg = consumer.receive();
                    System.out.println("[ACTIVE] Received: " + new String(msg.getData()));
                    consumer.acknowledge(msg);
                }

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
