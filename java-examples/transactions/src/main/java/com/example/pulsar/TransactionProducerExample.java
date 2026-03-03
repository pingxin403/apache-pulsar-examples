package com.example.pulsar;

import org.apache.pulsar.client.api.*;

/**
 * 事务 Producer 示例
 * 演示如何使用事务发送消息，确保消息的原子性
 */
public class TransactionProducerExample {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/transaction-topic";
    
    public static void main(String[] args) {
        try {
            // 创建 Pulsar 客户端，启用事务
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .enableTransaction(true)
                    .build();
            
            // 创建 Producer
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(TOPIC)
                    .sendTimeout(0, java.util.concurrent.TimeUnit.SECONDS)
                    .create();
            
            System.out.println("🚀 开始事务发送示例...");
            
            // 示例 1: 成功的事务
            System.out.println("\n📝 示例 1: 成功提交事务");
            Transaction txn1 = client.newTransaction()
                    .withTransactionTimeout(1, java.util.concurrent.TimeUnit.MINUTES)
                    .build()
                    .get();
            
            try {
                // 在事务中发送多条消息
                producer.newMessage(txn1)
                        .value("Transaction Message 1")
                        .send();
                System.out.println("   ✅ 发送消息 1");
                
                producer.newMessage(txn1)
                        .value("Transaction Message 2")
                        .send();
                System.out.println("   ✅ 发送消息 2");
                
                producer.newMessage(txn1)
                        .value("Transaction Message 3")
                        .send();
                System.out.println("   ✅ 发送消息 3");
                
                // 提交事务
                txn1.commit().get();
                System.out.println("   ✅ 事务提交成功，所有消息已持久化");
                
            } catch (Exception e) {
                // 发生错误时回滚事务
                txn1.abort().get();
                System.err.println("   ❌ 事务回滚");
                throw e;
            }
            
            // 示例 2: 回滚的事务
            System.out.println("\n📝 示例 2: 回滚事务");
            Transaction txn2 = client.newTransaction()
                    .withTransactionTimeout(1, java.util.concurrent.TimeUnit.MINUTES)
                    .build()
                    .get();
            
            try {
                producer.newMessage(txn2)
                        .value("Transaction Message 4")
                        .send();
                System.out.println("   ✅ 发送消息 4");
                
                producer.newMessage(txn2)
                        .value("Transaction Message 5")
                        .send();
                System.out.println("   ✅ 发送消息 5");
                
                // 模拟业务逻辑错误
                System.out.println("   ⚠️  模拟业务错误，准备回滚...");
                throw new RuntimeException("模拟业务错误");
                
            } catch (Exception e) {
                // 回滚事务
                txn2.abort().get();
                System.out.println("   ✅ 事务已回滚，消息 4 和 5 不会被消费");
            }
            
            // 关闭资源
            producer.close();
            client.close();
            
            System.out.println("\n✅ 事务 Producer 示例执行完成");
            System.out.println("💡 提示: 运行 TransactionConsumerExample 查看消费结果");
            
        } catch (Exception e) {
            System.err.println("❌ 执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
