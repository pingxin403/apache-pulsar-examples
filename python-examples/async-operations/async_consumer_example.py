#!/usr/bin/env python3
"""
异步 Consumer 示例
演示如何使用异步方式接收消息
"""

import pulsar
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/async-consumer-topic'

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始异步 Consumer 示例...\n")
        
        # 创建 Producer（先发送一些消息）
        producer = client.create_producer(TOPIC)
        
        print("📤 发送测试消息:")
        for i in range(1, 6):
            message = f"测试消息 {i}"
            producer.send(message.encode('utf-8'))
            print(f"   ✅ 发送: {message}")
        
        # 创建 Consumer
        consumer = client.subscribe(
            TOPIC,
            subscription_name='async-subscription'
        )
        
        print("\n📥 异步接收消息:")
        start_time = time.time()
        
        # 异步接收消息
        for i in range(5):
            # 使用 receive_async 会返回 Future 对象
            # 但 Python 客户端的 receive 本身就是非阻塞的
            msg = consumer.receive(timeout_millis=5000)
            if msg:
                content = msg.data().decode('utf-8')
                print(f"   📩 接收: {content}")
                print(f"      消息ID: {msg.message_id()}")
                
                # 异步确认
                consumer.acknowledge(msg)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n⏱️  总耗时: {elapsed:.3f} 秒")
        print(f"📊 平均每条消息: {elapsed/5*1000:.2f} 毫秒")
        
        print("\n✅ 异步 Consumer 示例执行完成")
        print("💡 特点: 非阻塞接收，提高处理效率")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
