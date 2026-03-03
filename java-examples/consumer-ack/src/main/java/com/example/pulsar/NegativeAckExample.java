package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * Negative ACK 示例
 * 对应文章: 01-入门篇/07-Consumer-ACK机制.md
 */
public class NegativeAckExample {
    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("persistent://public/default/my-topic")
            .subscriptionName("my-subscription")
            .subscriptionType(SubscriptionType.Shared)
            .negativeAckRedeliveryDelay(1, java.util.concurrent.TimeUnit.SECONDS)  // 1 秒后重新投递
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
                
            } catch (RetryableException e) {
                // 可重试的错误，发送 Nack
                System.err.println("⚠️ 消息处理失败，将重新投递: " + e.getMessage());
                consumer.negativeAcknowledge(msg);
                
            } catch (Exception e) {
                // 不可重试的错误，确认消息（避免无限重试）
                System.err.println("❌ 消息无法处理，已确认: " + e.getMessage());
                consumer.acknowledge(msg);
            }
        }
    }
    
    private static void processMessage(String content) throws Exception {
        // 模拟可重试的错误（如网络超时）
        if (content.contains("timeout")) {
            throw new RetryableException("网络超时");
        }
        // 模拟不可重试的错误（如数据格式错误）
        if (content.contains("invalid")) {
            throw new Exception("数据格式错误");
        }
        // 正常处理逻辑
    }
    
    static class RetryableException extends Exception {
        public RetryableException(String message) {
            super(message);
        }
    }
}
