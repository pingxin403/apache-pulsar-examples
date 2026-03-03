package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * Dead Letter Topic 示例
 * 演示如何使用死信队列处理失败的消息
 */
public class DeadLetterTopicExample {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/dlt-topic";
    private static final String DLT_TOPIC = "persistent://public/default/dlt-topic-DLQ";
    
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
            
            System.out.println("🚀 开始 Dead Letter Topic 示例...\n");
            
            // 发送测试消息
            System.out.println("📤 发送测试消息...");
            for (int i = 1; i <= 5; i++) {
                String message = "测试消息 " + i;
                producer.send(message);
                System.out.println("   ✅ 发送: " + message);
            }
            
            // 创建 Consumer，配置 Dead Letter Topic
            System.out.println("\n📥 创建 Consumer（配置 DLT）...");
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(TOPIC)
                    .subscriptionName("dlt-subscription")
                    .subscriptionType(SubscriptionType.Shared)
                    .negativeAckRedeliveryDelay(1, java.util.concurrent.TimeUnit.SECONDS)
                    .deadLetterPolicy(DeadLetterPolicy.builder()
                            .maxRedeliverCount(3)  // 最多重试 3 次
                            .deadLetterTopic(DLT_TOPIC)  // 死信队列 Topic
                            .build())
                    .subscribe();
            
            System.out.println("   ✅ Consumer 已创建");
            System.out.println("   ⚙️  最大重试次数: 3");
            System.out.println("   ⚙️  死信队列: " + DLT_TOPIC);
            
            // 处理消息
            System.out.println("\n🔄 开始处理消息...\n");
            int processedCount = 0;
            int maxMessages = 5;
            
            while (processedCount < maxMessages) {
                Message<String> message = consumer.receive(5, java.util.concurrent.TimeUnit.SECONDS);
                
                if (message == null) {
                    break;
                }
                
                processedCount++;
                String content = message.getValue();
                int redeliveryCount = message.getRedeliveryCount();
                
                System.out.println("📩 接收消息 " + processedCount + ":");
                System.out.println("   内容: " + content);
                System.out.println("   重试次数: " + redeliveryCount);
                
                // 模拟处理逻辑：消息 3 总是失败
                if (content.contains("消息 3")) {
                    System.out.println("   ❌ 处理失败，发送 Negative ACK");
                    consumer.negativeAcknowledge(message);
                    
                    if (redeliveryCount >= 2) {
                        System.out.println("   ⚠️  已达到最大重试次数，将进入死信队列");
                    }
                } else {
                    System.out.println("   ✅ 处理成功");
                    consumer.acknowledge(message);
                }
                
                System.out.println();
            }
            
            // 等待一段时间，让失败的消息进入死信队列
            System.out.println("⏳ 等待 5 秒，让失败消息进入死信队列...\n");
            Thread.sleep(5000);
            
            // 创建 DLT Consumer，处理死信消息
            System.out.println("📥 创建 DLT Consumer...");
            Consumer<String> dltConsumer = client.newConsumer(Schema.STRING)
                    .topic(DLT_TOPIC)
                    .subscriptionName("dlt-consumer")
                    .subscribe();
            
            System.out.println("   ✅ DLT Consumer 已创建\n");
            
            // 处理死信消息
            System.out.println("🔍 检查死信队列...");
            Message<String> dltMessage = dltConsumer.receive(3, java.util.concurrent.TimeUnit.SECONDS);
            
            if (dltMessage != null) {
                System.out.println("💀 发现死信消息:");
                System.out.println("   内容: " + dltMessage.getValue());
                System.out.println("   消息ID: " + dltMessage.getMessageId());
                System.out.println("   原始Topic: " + dltMessage.getTopicName());
                
                // 可以在这里进行特殊处理，如：
                // 1. 记录到日志
                // 2. 发送告警
                // 3. 人工介入处理
                // 4. 存储到数据库
                
                dltConsumer.acknowledge(dltMessage);
                System.out.println("   ✅ 死信消息已处理");
            } else {
                System.out.println("   ℹ️  死信队列为空");
            }
            
            // 关闭资源
            producer.close();
            consumer.close();
            dltConsumer.close();
            client.close();
            
            System.out.println("\n✅ Dead Letter Topic 示例执行完成");
            
        } catch (Exception e) {
            System.err.println("❌ 执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
