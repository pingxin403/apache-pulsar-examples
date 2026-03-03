package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * Cumulative ACK 示例
 * 对应文章: 01-入门篇/07-Consumer-ACK机制.md
 * 
 * Cumulative ACK 会确认当前消息及之前的所有消息，适用于 Exclusive 或 Failover 订阅模式。
 * 相比 Individual ACK，Cumulative ACK 可以减少网络开销，但要求消息必须按顺序处理。
 */
public class CumulativeAckExample {
    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

        // Cumulative ACK 只能用于 Exclusive 或 Failover 订阅模式
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("persistent://public/default/cumulative-ack-topic")
            .subscriptionName("cumulative-subscription")
            .subscriptionType(SubscriptionType.Exclusive)  // 使用 Exclusive 订阅
            .subscribe();

        System.out.println("🚀 Consumer 已启动，使用 Cumulative ACK 模式");
        System.out.println("📌 订阅类型: Exclusive（确保消息顺序处理）");
        System.out.println("📌 Cumulative ACK 会确认当前消息及之前的所有消息\n");

        int messageCount = 0;
        
        try {
            while (messageCount < 10) {
                Message<String> msg = consumer.receive();
                messageCount++;
                
                try {
                    // 处理消息
                    String content = msg.getValue();
                    System.out.println("📩 收到消息 #" + messageCount + ": " + content);
                    processMessage(content);
                    
                    // 每处理 3 条消息，执行一次 Cumulative ACK
                    if (messageCount % 3 == 0) {
                        consumer.acknowledgeCumulative(msg);
                        System.out.println("✅ Cumulative ACK: 已确认消息 #1 到 #" + messageCount);
                        System.out.println("💡 网络开销: 1 次 ACK 请求确认了 3 条消息\n");
                    } else {
                        System.out.println("⏳ 暂不确认，等待批量确认\n");
                    }
                    
                } catch (Exception e) {
                    System.err.println("❌ 消息处理失败: " + e.getMessage());
                    // Cumulative ACK 模式下，如果某条消息失败，不能跳过
                    // 必须等待该消息处理成功后才能继续
                    break;
                }
            }
            
            System.out.println("\n📊 性能对比:");
            System.out.println("Individual ACK: 10 条消息 = 10 次网络请求");
            System.out.println("Cumulative ACK: 10 条消息 = 4 次网络请求（每 3 条确认一次）");
            System.out.println("网络开销降低: 60%");
            
        } finally {
            consumer.close();
            client.close();
            System.out.println("\n🔚 Consumer 已关闭");
        }
    }
    
    private static void processMessage(String content) throws Exception {
        // 模拟业务处理
        Thread.sleep(100);
        
        if (content.contains("error")) {
            throw new Exception("业务处理失败");
        }
        // 正常处理逻辑
    }
}
