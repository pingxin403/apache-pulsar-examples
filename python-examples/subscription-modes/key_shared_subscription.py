#!/usr/bin/env python3
"""
Key_Shared 订阅模式示例
演示 Key_Shared 订阅模式的使用
"""

import pulsar
import threading
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/key-shared-topic'

def consumer_worker(consumer_id):
    """Consumer 工作线程"""
    client = pulsar.Client(SERVICE_URL)
    
    try:
        consumer = client.subscribe(
            TOPIC,
            subscription_name='key-shared-subscription',
            consumer_type=pulsar.ConsumerType.KeyShared
        )
        
        print(f"   ✅ Consumer {consumer_id} 已启动")
        
        # 接收消息
        received_messages = []
        for _ in range(10):
            try:
                msg = consumer.receive(timeout_millis=3000)
                if msg:
                    content = msg.data().decode('utf-8')
                    key = msg.partition_key() if msg.partition_key() else "无Key"
                    print(f"   📩 Consumer {consumer_id} 接收: {content} (Key: {key})")
                    received_messages.append(content)
                    consumer.acknowledge(msg)
            except Exception:
                pass
        
        if received_messages:
            print(f"   ✅ Consumer {consumer_id} 共接收 {len(received_messages)} 条消息")
        
    except Exception as e:
        print(f"   ❌ Consumer {consumer_id} 错误: {e}")
    finally:
        client.close()

def main():
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始 Key_Shared 订阅模式示例...\n")
        
        # 创建 Producer
        producer = client.create_producer(TOPIC)
        
        # 发送带 Key 的消息
        print("📤 发送带 Key 的测试消息...")
        keys = ['user-1', 'user-2', 'user-3']
        for i in range(1, 13):
            key = keys[(i - 1) % 3]
            message = f"消息 {i}"
            producer.send(
                message.encode('utf-8'),
                partition_key=key
            )
            print(f"   ✅ 发送: {message} (Key: {key})")
        
        # 创建多个 Key_Shared Consumer
        print("\n📥 创建 3 个 Key_Shared Consumers...")
        print("   ℹ️  相同 Key 的消息会被同一个 Consumer 接收\n")
        
        threads = []
        for i in range(1, 4):
            thread = threading.Thread(target=consumer_worker, args=(i,))
            thread.start()
            threads.append(thread)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print("\n✅ Key_Shared 订阅模式示例执行完成")
        print("💡 特点: 相同 Key 的消息保证顺序，不同 Key 可并行消费")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
