package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * Individual ACK 示例
 * 对应文章: 01-入门篇/07-Consumer-ACK机制.md
 */
public class IndividualAckExample {
    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("persistent://public/default/my-topic")
            .subscriptionName("my-subscription")
            .subscriptionType(SubscriptionType.Shared)  // Shared 订阅
            .subscribe();

        while (true) {
            Message<String> msg = consumer.receive();
            
            try {
                // 处理消息
                String content = msg.getValue();
                System.out.println("📩 收到消息: " + content);
                processMessage(content);
                
                // 处理成功，单独确认这条消息
                consumer.acknowledge(msg);
                System.out.println("✅ 消息已确认: " + msg.getMessageId());
                
            } catch (Exception e) {
                System.err.println("❌ 消息处理失败: " + e.getMessage());
                // 不确认，消息会被重新投递
            }
        }
    }
    
    private static void processMessage(String content) throws Exception {
        // 模拟业务处理
        if (content.contains("error")) {
            throw new Exception("业务处理失败");
        }
        // 正常处理逻辑
    }
}
