package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * Dead Letter Queue 示例
 * 对应文章: 01-入门篇/07-Consumer-ACK机制.md
 */
public class DeadLetterQueueExample {
    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("persistent://public/default/my-topic")
            .subscriptionName("my-subscription")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
            .deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(3)  // 最多重试 3 次
                .deadLetterTopic("persistent://public/default/my-topic-DLQ")  // 死信队列
                .build())
            .subscribe();

        while (true) {
            Message<String> msg = consumer.receive();
            
            try {
                // 处理消息
                String content = msg.getValue();
                System.out.println("📩 收到消息: " + content);
                processMessage(content);
                
                // 处理成功，确认
                consumer.acknowledge(msg);
                System.out.println("✅ 消息已确认");
                
            } catch (Exception e) {
                System.err.println("❌ 消息处理失败: " + e.getMessage());
                // 发送 Nack，触发重试
                consumer.negativeAcknowledge(msg);
                // 如果重试次数超过 3 次，消息会自动发送到 DLQ
            }
        }
    }
    
    private static void processMessage(String content) throws Exception {
        // 模拟无法处理的消息
        if (content.contains("poison")) {
            throw new Exception("毒消息，无法处理");
        }
        // 正常处理逻辑
    }
}
