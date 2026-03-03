#!/usr/bin/env python3
"""
Shared 订阅模式示例
演示共享订阅模式的使用
"""

import pulsar
import threading
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/shared-topic'

def consumer_worker(consumer_id):
    """Consumer 工作线程"""
    client = pulsar.Client(SERVICE_URL)
    
    try:
        consumer = client.subscribe(
            TOPIC,
            subscription_name='shared-subscription',
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        print(f"   ✅ Consumer {consumer_id} 已启动")
        
        # 接收消息
        for _ in range(5):
            msg = consumer.receive(timeout_millis=10000)
            if msg:
                content = msg.data().decode('utf-8')
                print(f"   📩 Consumer {consumer_id} 接收: {content}")
                time.sleep(0.1)  # 模拟处理时间
                consumer.acknowledge(msg)
        
        print(f"   ✅ Consumer {consumer_id} 完成")
        
    except Exception as e:
        print(f"   ❌ Consumer {consumer_id} 错误: {e}")
    finally:
        client.close()

def main():
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始 Shared 订阅模式示例...\n")
        
        # 创建 Producer
        producer = client.create_producer(TOPIC)
        
        # 发送测试消息
        print("📤 发送测试消息...")
        for i in range(1, 11):
            message = f"消息 {i}"
            producer.send(message.encode('utf-8'))
            print(f"   ✅ 发送: {message}")
        
        # 创建多个 Consumer
        print("\n📥 创建 3 个 Shared Consumers...")
        threads = []
        for i in range(1, 4):
            thread = threading.Thread(target=consumer_worker, args=(i,))
            thread.start()
            threads.append(thread)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print("\n✅ Shared 订阅模式示例执行完成")
        print("💡 特点: 多个 Consumer 共享消息，实现负载均衡")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
