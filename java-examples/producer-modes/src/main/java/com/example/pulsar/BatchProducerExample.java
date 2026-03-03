package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * 批量发送示例
 * 对应文章: 01-入门篇/06-Producer发送模式.md
 * 
 * 批量发送适用于高吞吐量、低延迟要求的场景，如 IoT 设备数据上报、日志收集等。
 * 优点：降低网络开销，提高吞吐量
 * 缺点：增加了消息延迟（需要等待批次填满或超时）
 */
public class BatchProducerExample {
    private static final Logger logger = Logger.getLogger(BatchProducerExample.class.getName());
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/iot-sensor-data")
                .enableBatching(true)  // 启用批量发送
                .batchingMaxMessages(100)  // 每批最多 100 条消息
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)  // 最多等待 10ms
                .create()) {
            
            logger.info("开始批量发送消息...");
            logger.info("批量配置 - 最大消息数: 100, 最大延迟: 10ms");
            
            // 发送 1000 条小消息
            for (int i = 0; i < 1000; i++) {
                producer.sendAsync("传感器数据 " + i);
                
                if (i % 200 == 0) {
                    logger.info("已发送 " + i + " 条消息");
                }
            }
            
            // 等待所有批次发送完成
            producer.flush();
            
            logger.info("✅ 所有消息已发送完成");
            
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
