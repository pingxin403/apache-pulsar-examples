package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * 订单事件 Producer - Key_Shared 示例
 * 
 * 演示如何使用 Key_Shared 订阅模式发送带有 Key 的消息。
 * 相同 Key 的消息会被路由到同一个 Consumer，保证顺序处理。
 * 
 * 对应文章: 03-核心功能深度篇/01-Key_Shared订阅实战.md
 */
public class OrderProducer {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/order-events";
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(TOPIC)
                .create()) {
            
            System.out.println("🚀 Producer 已启动，开始发送订单事件...\n");
            
            // 发送多个订单的事件，演示 Key_Shared 的负载均衡
            sendOrderEvents(producer, "order-12345");
            sendOrderEvents(producer, "order-67890");
            sendOrderEvents(producer, "order-11111");
            
            System.out.println("\n✅ 所有订单事件已发送完成");
            
        } catch (PulsarClientException e) {
            System.err.println("❌ Pulsar 客户端错误: " + e.getMessage());
            System.err.println("请确保 Pulsar 服务正在运行:");
            System.err.println("  docker-compose -f ../../docker-compose/docker-compose.yml up -d");
            System.exit(1);
        }
    }
    
    /**
     * 发送单个订单的所有事件
     * 
     * @param producer Pulsar Producer
     * @param orderId 订单 ID（作为消息的 Key）
     */
    private static void sendOrderEvents(Producer<String> producer, String orderId) 
            throws PulsarClientException {
        
        String[] events = {"订单创建", "订单支付", "订单发货"};
        
        for (String event : events) {
            MessageId messageId = producer.newMessage()
                    .key(orderId)  // 关键：指定 Key，确保同一订单的消息被同一个 Consumer 处理
                    .value(event)
                    .send();
            
            System.out.printf("📤 已发送 [订单: %s] [事件: %s] [MessageId: %s]%n", 
                    orderId, event, messageId);
            
            // 添加短暂延迟，便于观察消息顺序
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
