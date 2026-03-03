package com.example.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class ProducerExample {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "persistent://public/default/quickstart-topic";

        // Create Pulsar client
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            // Create producer
            try (Producer<byte[]> producer = client.newProducer()
                    .topic(topic)
                    .create()) {

                // Send 10 messages
                for (int i = 0; i < 10; i++) {
                    String message = "Hello Pulsar! Message " + i;
                    producer.send(message.getBytes());
                    System.out.println("Sent: " + message);
                }

                System.out.println("\nSuccessfully sent 10 messages!");

            }
        } catch (PulsarClientException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
