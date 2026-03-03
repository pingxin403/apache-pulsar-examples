#!/usr/bin/env python3
"""
同步发送示例
对应文章: 01-入门篇/06-Producer发送模式.md

同步发送适用于需要确保消息可靠送达的场景，如支付订单确认、重要通知等。
优点：可靠性高，能立即知道发送结果
缺点：吞吐量较低，每次发送都需要等待响应
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
    """主函数：演示同步发送消息"""
    client: Optional[pulsar.Client] = None
    producer: Optional[pulsar.Producer] = None
    
    try:
        # 创建 Pulsar 客户端
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Producer
        producer = client.create_producer(
            'persistent://public/default/payment-orders'
        )
        
        logger.info("开始同步发送消息...")
        
        # 同步发送消息
        msg_id = producer.send('订单 12345 支付成功'.encode('utf-8'))
        logger.info(f"✅ 消息发送成功，MessageId: {msg_id}")
        
        # 再发送几条消息
        for i in range(1, 6):
            order_id = 12345 + i
            message = f'订单 {order_id} 支付成功'.encode('utf-8')
            msg_id = producer.send(message)
            logger.info(f"✅ 订单 {order_id} 消息发送成功，MessageId: {msg_id}")
        
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
