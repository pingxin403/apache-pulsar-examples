package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * 带 Key 路由示例
 * 对应文章: 01-入门篇/06-Producer发送模式.md
 * 
 * 带 Key 路由适用于需要保证消息顺序的场景，如用户行为分析、订单状态机等。
 * 优点：保证相同 Key 的消息发送到同一个分区，确保顺序性
 * 缺点：可能导致分区负载不均衡
 */
public class KeyBasedProducerExample {
    private static final Logger logger = Logger.getLogger(KeyBasedProducerExample.class.getName());
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/user-orders")
                .create()) {
            
            logger.info("开始发送带 Key 的消息...");
            
            // 用户 A 的订单事件（使用用户 ID 作为 Key）
            // 这些消息会被路由到同一个分区，保证顺序性
            MessageId msgId1 = producer.newMessage()
                .key("user-12345")  // 关键：指定 Key
                .value("用户 12345 创建订单")
                .send();
            logger.info("✅ 发送消息 1: " + msgId1);
            
            MessageId msgId2 = producer.newMessage()
                .key("user-12345")
                .value("用户 12345 支付订单")
                .send();
            logger.info("✅ 发送消息 2: " + msgId2);
            
            MessageId msgId3 = producer.newMessage()
                .key("user-12345")
                .value("用户 12345 确认收货")
                .send();
            logger.info("✅ 发送消息 3: " + msgId3);
            
            // 用户 B 的订单事件（不同的 Key，可能路由到不同分区）
            MessageId msgId4 = producer.newMessage()
                .key("user-67890")
                .value("用户 67890 创建订单")
                .send();
            logger.info("✅ 发送消息 4: " + msgId4);
            
            logger.info("✅ 所有订单事件已发送完成");
            
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, "❌ Pulsar 客户端错误", e);
            System.exit(1);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ 发生未预期的错误", e);
            System.exit(1);
        }
        
        logger.info("程序执行完成");
    }
}
