package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

/**
 * JSON Schema 示例
 * 演示如何使用 JSON Schema 发送和接收消息
 */
public class JsonSchemaExample {
    
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/json-schema-topic";
    
    public static void main(String[] args) {
        try {
            // 创建 Pulsar 客户端
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            
            // 使用 JSON Schema 创建 Producer
            Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
                    .topic(TOPIC)
                    .create();
            
            // 发送消息
            User user = new User("Bob", 25, "bob@example.com");
            MessageId msgId = producer.send(user);
            System.out.println("✅ 发送消息成功: " + msgId);
            System.out.println("   用户信息: " + user);
            
            // 使用 JSON Schema 创建 Consumer
            Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class))
                    .topic(TOPIC)
                    .subscriptionName("json-subscription")
                    .subscribe();
            
            // 接收消息
            Message<User> message = consumer.receive();
            User receivedUser = message.getValue();
            System.out.println("📩 接收消息成功:");
            System.out.println("   用户信息: " + receivedUser);
            
            // 确认消息
            consumer.acknowledge(message);
            
            // 关闭资源
            producer.close();
            consumer.close();
            client.close();
            
            System.out.println("✅ JSON Schema 示例执行完成");
            
        } catch (Exception e) {
            System.err.println("❌ 执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
