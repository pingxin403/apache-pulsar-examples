package com.example.pulsar;

import org.apache.pulsar.client.api.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * 延迟消息示例
 * 演示如何使用 Pulsar 的延迟消息功能
 */
public class DelayedMessageExample {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/delayed-topic";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    public static void main(String[] args) {
        try {
            // 创建 Pulsar 客户端
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            
            // 创建 Producer
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(TOPIC)
                    .create();
            
            // 创建 Consumer
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(TOPIC)
                    .subscriptionName("delayed-subscription")
                    .subscribe();
            
            System.out.println("🚀 开始延迟消息示例...\n");
            
            // 示例 1: 延迟 5 秒
            System.out.println("📝 示例 1: 延迟 5 秒");
            String sendTime1 = getCurrentTime();
            producer.newMessage()
                    .value("延迟 5 秒的消息")
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
            System.out.println("   ✅ 发送时间: " + sendTime1);
            System.out.println("   ⏰ 预计接收时间: " + getDelayedTime(5));
            
            // 示例 2: 延迟 10 秒
            System.out.println("\n📝 示例 2: 延迟 10 秒");
            String sendTime2 = getCurrentTime();
            producer.newMessage()
                    .value("延迟 10 秒的消息")
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            System.out.println("   ✅ 发送时间: " + sendTime2);
            System.out.println("   ⏰ 预计接收时间: " + getDelayedTime(10));
            
            // 示例 3: 延迟 15 秒
            System.out.println("\n📝 示例 3: 延迟 15 秒");
            String sendTime3 = getCurrentTime();
            producer.newMessage()
                    .value("延迟 15 秒的消息")
                    .deliverAfter(15, TimeUnit.SECONDS)
                    .send();
            System.out.println("   ✅ 发送时间: " + sendTime3);
            System.out.println("   ⏰ 预计接收时间: " + getDelayedTime(15));
            
            // 示例 4: 立即发送（不延迟）
            System.out.println("\n📝 示例 4: 立即发送（不延迟）");
            String sendTime4 = getCurrentTime();
            producer.newMessage()
                    .value("立即发送的消息")
                    .send();
            System.out.println("   ✅ 发送时间: " + sendTime4);
            
            System.out.println("\n⏳ 等待接收消息...\n");
            
            // 接收消息
            for (int i = 0; i < 4; i++) {
                Message<String> message = consumer.receive(20, TimeUnit.SECONDS);
                if (message != null) {
                    String receiveTime = getCurrentTime();
                    System.out.println("📩 接收消息 " + (i + 1) + ":");
                    System.out.println("   内容: " + message.getValue());
                    System.out.println("   接收时间: " + receiveTime);
                    System.out.println("   消息ID: " + message.getMessageId());
                    
                    // 确认消息
                    consumer.acknowledge(message);
                    System.out.println();
                }
            }
            
            // 关闭资源
            producer.close();
            consumer.close();
            client.close();
            
            System.out.println("✅ 延迟消息示例执行完成");
            
        } catch (Exception e) {
            System.err.println("❌ 执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 获取当前时间
     */
    private static String getCurrentTime() {
        return LocalDateTime.now().format(formatter);
    }
    
    /**
     * 获取延迟后的时间
     */
    private static String getDelayedTime(int seconds) {
        return LocalDateTime.now().plusSeconds(seconds).format(formatter);
    }
}
