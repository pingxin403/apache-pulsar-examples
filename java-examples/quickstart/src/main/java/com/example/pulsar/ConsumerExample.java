package com.example.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class ConsumerExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/quickstart-topic";
        String subscription = "quickstart-subscription";

        // Create Pulsar client
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Create consumer
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe()) {

                System.out.println("Waiting for messages...\n");

                // Receive 10 messages
                for (int i = 0; i < 10; i++) {
                    Message<byte[]> message = consumer.receive();
                    try {
                        String content = new String(message.getData());
                        System.out.println("Received: " + content);
                        
                        // Acknowledge the message
                        consumer.acknowledge(message);
                    } catch (Exception e) {
                        // Negative acknowledge if processing fails
                        consumer.negativeAcknowledge(message);
                    }
                }

                System.out.println("\nSuccessfully received 10 messages!");

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
