package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * 订单事件 Consumer - Key_Shared 示例
 * 
 * 演示如何使用 Key_Shared 订阅模式消费消息。
 * 多个 Consumer 可以并行处理，但相同 Key 的消息始终由同一个 Consumer 处理，保证顺序。
 * 
 * 对应文章: 03-核心功能深度篇/01-Key_Shared订阅实战.md
 */
public class OrderConsumer {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/order-events";
    private static final String SUBSCRIPTION_NAME = "order-processing";
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
             Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)  // 关键：Key_Shared 模式
                .consumerName("consumer-" + System.currentTimeMillis())  // 唯一的 Consumer 名称
                .receiverQueueSize(100)  // 接收队列大小，影响重新平衡速度
                .subscribe()) {
            
            System.out.println("🔄 Consumer 已启动: " + consumer.getConsumerName());
            System.out.println("📌 订阅类型: Key_Shared");
            System.out.println("📌 订阅名称: " + SUBSCRIPTION_NAME);
            System.out.println("📌 等待消息...\n");
            
            // 持续接收消息
            while (true) {
                try {
                    // 接收消息（阻塞等待）
                    Message<String> msg = consumer.receive();
                    
                    // 获取消息的 Key 和内容
                    String key = msg.getKey() != null ? msg.getKey() : "no-key";
                    String value = msg.getValue();
                    
                    System.out.printf("📩 [%s] 收到消息 [订单: %s] [事件: %s]%n", 
                            consumer.getConsumerName(), key, value);
                    
                    // 模拟业务处理（例如：更新订单状态、发送通知等）
                    processOrder(key, value);
                    
                    // 确认消息已处理
                    consumer.acknowledge(msg);
                    
                } catch (PulsarClientException e) {
                    System.err.println("❌ 接收消息失败: " + e.getMessage());
                    // 继续处理下一条消息
                }
            }
            
        } catch (PulsarClientException e) {
            System.err.println("❌ Pulsar 客户端错误: " + e.getMessage());
            System.err.println("请确保 Pulsar 服务正在运行:");
            System.err.println("  docker-compose -f ../../docker-compose/docker-compose.yml up -d");
            System.exit(1);
        }
    }
    
    /**
     * 处理订单事件的业务逻辑
     * 
     * @param orderId 订单 ID
     * @param event 订单事件
     */
    private static void processOrder(String orderId, String event) {
        try {
            // 模拟业务处理时间
            Thread.sleep(100);
            
            System.out.printf("   ✓ 处理完成 [订单: %s] [事件: %s]%n", orderId, event);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("   ✗ 处理被中断");
        }
    }
}
