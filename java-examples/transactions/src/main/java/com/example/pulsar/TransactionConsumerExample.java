package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * 事务 Consumer 示例
 * 演示如何在事务中消费和确认消息
 */
public class TransactionConsumerExample {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/transaction-topic";
    
    public static void main(String[] args) {
        try {
            // 创建 Pulsar 客户端，启用事务
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .enableTransaction(true)
                    .build();
            
            // 创建 Consumer
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(TOPIC)
                    .subscriptionName("transaction-subscription")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
            
            System.out.println("🚀 开始事务消费示例...");
            System.out.println("💡 等待接收消息（按 Ctrl+C 退出）...\n");
            
            int messageCount = 0;
            int maxMessages = 5; // 最多接收 5 条消息后退出
            
            while (messageCount < maxMessages) {
                // 接收消息（10秒超时）
                Message<String> message = consumer.receive(10, java.util.concurrent.TimeUnit.SECONDS);
                
                if (message == null) {
                    System.out.println("⏰ 10秒内未收到消息，退出...");
                    break;
                }
                
                messageCount++;
                System.out.println("📩 接收消息 " + messageCount + ":");
                System.out.println("   内容: " + message.getValue());
                System.out.println("   消息ID: " + message.getMessageId());
                
                // 创建事务
                Transaction txn = client.newTransaction()
                        .withTransactionTimeout(1, java.util.concurrent.TimeUnit.MINUTES)
                        .build()
                        .get();
                
                try {
                    // 在事务中确认消息
                    consumer.acknowledgeAsync(message.getMessageId(), txn).get();
                    System.out.println("   ✅ 在事务中确认消息");
                    
                    // 模拟业务处理
                    processMessage(message.getValue());
                    
                    // 提交事务
                    txn.commit().get();
                    System.out.println("   ✅ 事务提交成功\n");
                    
                } catch (Exception e) {
                    // 发生错误时回滚事务
                    txn.abort().get();
                    System.err.println("   ❌ 事务回滚，消息将被重新投递\n");
                }
            }
            
            // 关闭资源
            consumer.close();
            client.close();
            
            System.out.println("✅ 事务 Consumer 示例执行完成");
            System.out.println("📊 共处理 " + messageCount + " 条消息");
            
        } catch (Exception e) {
            System.err.println("❌ 执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 模拟业务处理逻辑
     */
    private static void processMessage(String message) throws Exception {
        // 模拟业务处理
        System.out.println("   🔄 处理消息: " + message);
        Thread.sleep(100);
    }
}
