#!/usr/bin/env python3
"""
Individual ACK 示例
对应文章: 01-入门篇/07-Consumer-ACK机制.md

Individual ACK（单条确认）是最常用的确认方式，适合 Shared 订阅模式。
每条消息单独确认，失败的消息不影响其他消息的处理。
"""

import pulsar
import time
from typing import Optional


def process_message(content: str) -> None:
    """
    处理消息的业务逻辑
    
    Args:
        content: 消息内容
        
    Raises:
        Exception: 当消息包含 "error" 时抛出异常
    """
    if "error" in content:
        raise Exception("业务处理失败")
    print(f"✅ 消息处理成功: {content}")


def main() -> None:
    """Individual ACK 示例主函数"""
    # 创建 Pulsar 客户端
    client: Optional[pulsar.Client] = None
    consumer: Optional[pulsar.Consumer] = None
    
    try:
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Consumer，使用 Shared 订阅模式
        consumer = client.subscribe(
            'persistent://public/default/individual-ack-topic',
            'individual-ack-subscription',
            consumer_type=pulsar.ConsumerType.Shared  # Shared 订阅，支持多个 Consumer 并行消费
        )
        
        print("🚀 Consumer 已启动，使用 Individual ACK 模式")
        print("📌 订阅类型: Shared（支持多个 Consumer 并行消费）")
        print("📌 Individual ACK 会单独确认每条消息\n")
        
        message_count = 0
        max_messages = 10
        
        while message_count < max_messages:
            # 接收消息（阻塞等待）
            msg = consumer.receive()
            message_count += 1
            
            try:
                # 处理消息
                content = msg.data().decode('utf-8')
                print(f"📩 收到消息 #{message_count}: {content}")
                process_message(content)
                
                # 处理成功，单独确认这条消息
                consumer.acknowledge(msg)
                print(f"✅ 消息已确认: {msg.message_id()}\n")
                
            except Exception as e:
                # 处理失败，不确认消息
                print(f"❌ 消息处理失败: {e}")
                print(f"⚠️ 消息未确认，将被重新投递\n")
                # 注意：不调用 acknowledge()，消息会在 ACK 超时后重新投递
        
        print("\n📊 Individual ACK 特点:")
        print("✅ 精确控制：可以单独确认或拒绝每条消息")
        print("✅ 灵活性高：适合 Shared 订阅模式")
        print("✅ 容错性好：失败的消息不影响其他消息")
        print("❌ 网络开销：每条消息都需要一次 ACK 请求")
        
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
