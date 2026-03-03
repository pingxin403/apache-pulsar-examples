#!/usr/bin/env python3
"""
Failover 订阅模式示例
演示故障转移订阅模式的使用
"""

import pulsar
import threading
import time

SERVICE_URL = 'pulsar://localhost:6650'
TOPIC = 'persistent://public/default/failover-topic'

def consumer_worker(consumer_id):
    """Consumer 工作线程"""
    client = pulsar.Client(SERVICE_URL)
    
    try:
        consumer = client.subscribe(
            TOPIC,
            subscription_name='failover-subscription',
            consumer_type=pulsar.ConsumerType.Failover
        )
        
        print(f"   ✅ Consumer {consumer_id} 已启动")
        
        # 尝试接收消息
        received_count = 0
        for _ in range(10):
            try:
                msg = consumer.receive(timeout_millis=2000)
                if msg:
                    content = msg.data().decode('utf-8')
                    print(f"   📩 Consumer {consumer_id} 接收: {content}")
                    received_count += 1
                    consumer.acknowledge(msg)
            except Exception:
                pass
        
        if received_count > 0:
            print(f"   ✅ Consumer {consumer_id} 共接收 {received_count} 条消息（主Consumer）")
        else:
            print(f"   ℹ️  Consumer {consumer_id} 未接收到消息（备用Consumer）")
        
    except Exception as e:
        print(f"   ❌ Consumer {consumer_id} 错误: {e}")
    finally:
        client.close()

def main():
    client = pulsar.Client(SERVICE_URL)
    
    try:
        print("🚀 开始 Failover 订阅模式示例...\n")
        
        # 创建 Producer
        producer = client.create_producer(TOPIC)
        
        # 发送测试消息
        print("📤 发送测试消息...")
        for i in range(1, 11):
            message = f"消息 {i}"
            producer.send(message.encode('utf-8'))
            print(f"   ✅ 发送: {message}")
        
        # 创建多个 Failover Consumer
        print("\n📥 创建 3 个 Failover Consumers...")
        print("   ℹ️  只有主 Consumer 会接收消息，其他作为备用\n")
        
        threads = []
        for i in range(1, 4):
            thread = threading.Thread(target=consumer_worker, args=(i,))
            thread.start()
            threads.append(thread)
            time.sleep(0.5)  # 错开启动时间
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print("\n✅ Failover 订阅模式示例执行完成")
        print("💡 特点: 主 Consumer 故障时，备用 Consumer 自动接管")
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    main()
