#!/usr/bin/env python3
"""
Negative ACK 示例
对应文章: 01-入门篇/07-Consumer-ACK机制.md

Negative ACK（否定确认）明确告知 Pulsar 消息处理失败，触发快速重试。
适合处理临时性错误（如网络超时、服务暂时不可用）。
"""

import pulsar
import time
from typing import Optional


class RetryableException(Exception):
    """可重试的异常类型"""
    pass


def process_message(content: str) -> None:
    """
    处理消息的业务逻辑
    
    Args:
        content: 消息内容
        
    Raises:
        RetryableException: 可重试的错误（如网络超时）
        Exception: 不可重试的错误（如数据格式错误）
    """
    if "timeout" in content:
        raise RetryableException("网络超时")
    if "invalid" in content:
        raise Exception("数据格式错误")
    # 正常处理逻辑
    print(f"✅ 消息处理成功: {content}")


def main() -> None:
    """Negative ACK 示例主函数"""
    # 创建 Pulsar 客户端
    client: Optional[pulsar.Client] = None
    consumer: Optional[pulsar.Consumer] = None
    
    try:
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Consumer，配置 Negative ACK 重新投递延迟
        consumer = client.subscribe(
            'persistent://public/default/negative-ack-topic',
            'negative-ack-subscription',
            consumer_type=pulsar.ConsumerType.Shared,
            negative_ack_redelivery_delay_ms=1000  # 1 秒后重新投递
        )
        
        print("🚀 Consumer 已启动，使用 Negative ACK 模式")
        print("📌 订阅类型: Shared")
        print("📌 Negative ACK 会触发消息快速重新投递（1 秒延迟）\n")
        
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
                
                # 处理成功，确认
                consumer.acknowledge(msg)
                print(f"✅ 消息已确认\n")
                
            except RetryableException as e:
                # 可重试的错误，发送 Nack
                print(f"⚠️ 消息处理失败，将重新投递: {e}")
                consumer.negative_acknowledge(msg)
                print("🔄 已发送 Negative ACK，消息将在 1 秒后重新投递\n")
                
            except Exception as e:
                # 不可重试的错误，确认消息（避免无限重试）
                print(f"❌ 消息无法处理，已确认: {e}")
                consumer.acknowledge(msg)
                print("💡 不可重试的错误，确认消息以避免无限重试\n")
        
        print("📊 Negative ACK 特点:")
        print("✅ 快速重试：立即触发消息重新投递")
        print("✅ 明确语义：区分'处理失败'和'不确认'")
        print("✅ 可配置延迟：控制重试间隔")
        print("❌ 需要判断：需要区分可重试和不可重试错误")
        
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
