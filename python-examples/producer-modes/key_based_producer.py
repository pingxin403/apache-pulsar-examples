#!/usr/bin/env python3
"""
带 Key 路由示例
对应文章: 01-入门篇/06-Producer发送模式.md

带 Key 路由适用于需要保证消息顺序的场景，如用户行为分析、订单状态机等。
优点：保证相同 Key 的消息顺序，支持 Key_Shared 订阅模式
缺点：可能导致分区负载不均衡
"""

import pulsar
import logging
from typing import Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def main() -> None:
    """主函数：演示带 Key 路由的消息发送"""
    client: Optional[pulsar.Client] = None
    producer: Optional[pulsar.Producer] = None
    
    try:
        # 创建 Pulsar 客户端
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Producer
        producer = client.create_producer(
            'persistent://public/default/user-orders'
        )
        
        logger.info("开始发送带 Key 的消息...")
        
        # 用户 A 的订单事件（相同 Key 保证顺序）
        user_a_key = 'user-12345'
        producer.send(
            '用户 12345 创建订单'.encode('utf-8'),
            partition_key=user_a_key
        )
        logger.info(f"✅ 发送消息: 用户 12345 创建订单 (Key: {user_a_key})")
        
        producer.send(
            '用户 12345 支付订单'.encode('utf-8'),
            partition_key=user_a_key
        )
        logger.info(f"✅ 发送消息: 用户 12345 支付订单 (Key: {user_a_key})")
        
        producer.send(
            '用户 12345 确认收货'.encode('utf-8'),
            partition_key=user_a_key
        )
        logger.info(f"✅ 发送消息: 用户 12345 确认收货 (Key: {user_a_key})")
        
        # 用户 B 的订单事件（不同 Key，可能路由到不同分区）
        user_b_key = 'user-67890'
        producer.send(
            '用户 67890 创建订单'.encode('utf-8'),
            partition_key=user_b_key
        )
        logger.info(f"✅ 发送消息: 用户 67890 创建订单 (Key: {user_b_key})")
        
        logger.info("✅ 所有订单事件已发送")
        
    except pulsar.ConnectError as e:
        logger.error(f"❌ 连接 Pulsar 失败: {e}")
        logger.error("请确保 Pulsar 服务正在运行:")
        logger.error("  docker-compose -f docker-compose/docker-compose.yml up -d")
        exit(1)
    except Exception as e:
        logger.error(f"❌ 发生未预期的错误: {e}")
        exit(1)
    finally:
        # 确保资源正确释放
        if producer:
            producer.close()
        if client:
            client.close()
        logger.info("程序执行完成")


if __name__ == '__main__':
    main()
