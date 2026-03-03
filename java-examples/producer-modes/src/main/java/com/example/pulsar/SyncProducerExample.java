package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * 同步发送示例
 * 对应文章: 01-入门篇/06-Producer发送模式.md
 * 
 * 同步发送适用于需要确保消息可靠送达的场景，如支付订单确认、重要通知等。
 * 优点：可靠性高，能立即知道发送结果
 * 缺点：吞吐量较低，每次发送都需要等待响应
 */
public class SyncProducerExample {
    private static final Logger logger = Logger.getLogger(SyncProducerExample.class.getName());
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/payment-orders")
                .create()) {
            
            logger.info("开始同步发送消息...");
            
            // 同步发送消息
            MessageId msgId = producer.send("订单 12345 支付成功");
            logger.info("✅ 消息发送成功，MessageId: " + msgId);
            
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, "❌ 消息发送失败", e);
            // 可以进行重试或记录日志
            System.exit(1);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ 发生未预期的错误", e);
            System.exit(1);
        }
        
        logger.info("程序执行完成");
    }
}
