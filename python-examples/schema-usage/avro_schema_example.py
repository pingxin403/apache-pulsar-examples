#!/usr/bin/env python3
"""
Avro Schema 示例
演示如何使用 Avro Schema 发送和接收消息
"""

import pulsar
from pulsar.schema import AvroSchema, Record, String, Integer

# 定义 User 类
class User(Record):
    name = String()
    age = Integer()
    email = String()

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/avro-schema-topic'

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始 Avro Schema 示例...\n")
        
        # 创建 Producer（使用 Avro Schema）
        producer = client.create_producer(
            TOPIC,
            schema=AvroSchema(User)
        )
        print("✅ Producer 已创建（Avro Schema）\n")
        
        # 发送消息
        print("📤 发送消息:")
        users = [
            User(name='Alice', age=30, email='alice@example.com'),
            User(name='Bob', age=25, email='bob@example.com'),
            User(name='Charlie', age=35, email='charlie@example.com')
        ]
        
        for user in users:
            producer.send(user)
            print(f"   ✅ 发送: {user.name}, {user.age}, {user.email}")
        
        # 创建 Consumer（使用 Avro Schema）
        consumer = client.subscribe(
            TOPIC,
            subscription_name='avro-subscription',
            schema=AvroSchema(User)
        )
        print("\n✅ Consumer 已创建（Avro Schema）\n")
        
        # 接收消息
        print("📩 接收消息:")
        for i in range(3):
            msg = consumer.receive(timeout_millis=5000)
            if msg:
                user = msg.value()
                print(f"   ✅ 接收: {user.name}, {user.age}, {user.email}")
                print(f"      消息ID: {msg.message_id()}")
                consumer.acknowledge(msg)
        
        print("\n✅ Avro Schema 示例执行完成")
        print("💡 特点: 二进制格式，高效压缩，强类型检查")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == '__main__':
    main()
