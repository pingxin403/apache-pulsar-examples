package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 异步发送示例
 * 对应文章: 01-入门篇/06-Producer发送模式.md
 * 
 * 异步发送适用于高吞吐量场景，如用户行为日志、实时监控数据等。
 * 优点：吞吐量高，不阻塞主线程
 * 缺点：需要处理回调，错误处理相对复杂
 */
public class AsyncProducerExample {
    private static final Logger logger = Logger.getLogger(AsyncProducerExample.class.getName());
    
    public static void main(String[] args) {
        // 使用 try-with-resources 确保资源自动释放
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/user-behavior-logs")
                .create()) {
            
            logger.info("开始异步发送消息...");
            
            // 统计成功和失败的消息数
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            
            // 异步发送 1000 条消息
            for (int i = 0; i < 1000; i++) {
                final int msgNum = i;
                producer.sendAsync("用户点击事件 " + i)
                    .thenAccept(msgId -> {
                        successCount.incrementAndGet();
                        if (msgNum % 100 == 0) {
                            logger.info("✅ 消息 " + msgNum + " 发送成功: " + msgId);
                        }
                    })
                    .exceptionally(ex -> {
                        failureCount.incrementAndGet();
                        logger.log(Level.WARNING, "❌ 消息 " + msgNum + " 发送失败", ex);
                        return null;
                    });
            }
            
            // 等待所有异步消息发送完成
            producer.flush();
            
            logger.info(String.format("发送完成 - 成功: %d, 失败: %d", 
                successCount.get(), failureCount.get()));
            
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
