#!/usr/bin/env python3
"""
Exclusive 订阅模式示例
演示独占订阅模式的使用
"""

import pulsar
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/exclusive-topic'

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始 Exclusive 订阅模式示例...\n")
        
        # 创建 Producer
        producer = client.create_producer(TOPIC)
        
        # 发送测试消息
        print("📤 发送测试消息...")
        for i in range(1, 6):
            message = f"消息 {i}"
            producer.send(message.encode('utf-8'))
            print(f"   ✅ 发送: {message}")
        
        # 创建 Exclusive Consumer
        print("\n📥 创建 Exclusive Consumer...")
        consumer = client.subscribe(
            TOPIC,
            subscription_name='exclusive-subscription',
            consumer_type=pulsar.ConsumerType.Exclusive
        )
        print("   ✅ Consumer 已创建（Exclusive 模式）")
        print("   ℹ️  只有一个 Consumer 可以接收消息\n")
        
        # 接收消息
        print("📩 接收消息:")
        for i in range(5):
            msg = consumer.receive(timeout_millis=5000)
            if msg:
                content = msg.data().decode('utf-8')
                print(f"   ✅ 接收: {content}")
                print(f"      消息ID: {msg.message_id()}")
                consumer.acknowledge(msg)
        
        print("\n✅ Exclusive 订阅模式示例执行完成")
        print("💡 特点: 只有一个 Consumer 可以消费，保证消息顺序")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
