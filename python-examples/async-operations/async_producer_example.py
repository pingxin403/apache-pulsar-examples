#!/usr/bin/env python3
"""
异步 Producer 示例
演示如何使用异步方式发送消息
"""

import pulsar
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/async-producer-topic'

def callback(res, msg_id):
    """发送回调函数"""
    print(f"   ✅ 消息发送成功: {msg_id}")

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始异步 Producer 示例...\n")
        
        # 创建 Producer
        producer = client.create_producer(TOPIC)
        
        # 异步发送消息
        print("📤 异步发送消息:")
        start_time = time.time()
        
        for i in range(1, 11):
            message = f"异步消息 {i}"
            # 异步发送，不等待响应
            producer.send_async(
                message.encode('utf-8'),
                callback=callback
            )
            print(f"   📨 提交发送: {message}")
        
        # 等待所有异步发送完成
        producer.flush()
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n⏱️  总耗时: {elapsed:.3f} 秒")
        print(f"📊 平均每条消息: {elapsed/10*1000:.2f} 毫秒")
        
        print("\n✅ 异步 Producer 示例执行完成")
        print("💡 特点: 不阻塞，高吞吐量，适合批量发送")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
