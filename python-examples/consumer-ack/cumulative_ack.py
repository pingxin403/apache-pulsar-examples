#!/usr/bin/env python3
"""
Cumulative ACK 示例
对应文章: 01-入门篇/07-Consumer-ACK机制.md

Cumulative ACK（累积确认）会确认当前消息及之前的所有消息，适合顺序处理场景。
注意：只能用于 Exclusive 或 Failover 订阅模式。
"""

import pulsar
import time
from typing import Optional


def process_message(content: str) -> None:
    """
    处理消息的业务逻辑
    
    Args:
        content: 消息内容
    """
    # 模拟消息处理
    time.sleep(0.1)
    print(f"✅ 消息处理成功: {content}")


def main() -> None:
    """Cumulative ACK 示例主函数"""
    # 创建 Pulsar 客户端
    client: Optional[pulsar.Client] = None
    consumer: Optional[pulsar.Consumer] = None
    
    try:
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Consumer，使用 Exclusive 订阅模式
        consumer = client.subscribe(
            'persistent://public/default/cumulative-ack-topic',
            'cumulative-ack-subscription',
            consumer_type=pulsar.ConsumerType.Exclusive  # 必须使用 Exclusive 或 Failover
        )
        
        print("🚀 Consumer 已启动，使用 Cumulative ACK 模式")
        print("📌 订阅类型: Exclusive（确保消息顺序处理）")
        print("📌 Cumulative ACK 会确认当前消息及之前的所有消息\n")
        
        message_count = 0
        max_messages = 10
        batch_size = 3  # 每处理 3 条消息，执行一次 Cumulative ACK
        
        while message_count < max_messages:
            # 接收消息（阻塞等待）
            msg = consumer.receive()
            message_count += 1
            
            # 处理消息
            content = msg.data().decode('utf-8')
            print(f"📩 收到消息 #{message_count}: {content}")
            process_message(content)
            
            # 每处理 batch_size 条消息，执行一次 Cumulative ACK
            if message_count % batch_size == 0:
                consumer.acknowledge_cumulative(msg)
                print(f"✅ Cumulative ACK: 已确认消息 #1 到 #{message_count}")
                print(f"💡 网络开销: 1 次 ACK 请求确认了 {batch_size} 条消息\n")
            else:
                print("⏳ 暂不确认，等待批量确认\n")
        
        # 确认剩余的消息
        if message_count % batch_size != 0:
            consumer.acknowledge_cumulative(msg)
            print(f"✅ Cumulative ACK: 已确认剩余消息\n")
        
        print("📊 性能对比:")
        print(f"Individual ACK: {max_messages} 条消息 = {max_messages} 次网络请求")
        ack_count = (max_messages // batch_size) + (1 if max_messages % batch_size != 0 else 0)
        print(f"Cumulative ACK: {max_messages} 条消息 = {ack_count} 次网络请求（每 {batch_size} 条确认一次）")
        reduction = (1 - ack_count / max_messages) * 100
        print(f"网络开销降低: {reduction:.0f}%")
        
        print("\n📊 Cumulative ACK 特点:")
        print("✅ 高效率：减少网络开销（批量确认）")
        print("✅ 简单易用：适合顺序处理场景")
        print("❌ 仅限 Exclusive/Failover：不支持 Shared 订阅")
        print("❌ 容错性差：某条消息失败会阻塞后续消息")
        
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        
    finally:
        # 关闭资源
        if consumer:
            consumer.close()
        if client:
            client.close()
        print("\n🔚 Consumer 已关闭")


if __name__ == "__main__":
    main()
