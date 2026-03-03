#!/usr/bin/env python3
"""
异步发送示例
对应文章: 01-入门篇/06-Producer发送模式.md

异步发送适用于高吞吐量场景，如用户行为日志、实时监控数据等。
优点：吞吐量高，不阻塞主线程
缺点：需要处理回调，错误处理相对复杂
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


def send_callback(res: pulsar.Result, msg_id: pulsar.MessageId) -> None:
    """
    异步发送回调函数
    
    Args:
        res: 发送结果
        msg_id: 消息ID
    """
    if res == pulsar.Result.Ok:
        logger.debug(f"✅ 消息发送成功: {msg_id}")
    else:
        logger.error(f"❌ 消息发送失败: {res}")


def main() -> None:
    """主函数：演示异步发送消息"""
    client: Optional[pulsar.Client] = None
    producer: Optional[pulsar.Producer] = None
    
    try:
        # 创建 Pulsar 客户端
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Producer
        producer = client.create_producer(
            'persistent://public/default/user-behavior-logs'
        )
        
        logger.info("开始异步发送消息...")
        
        # 异步发送 1000 条消息
        for i in range(1000):
            message = f'用户点击事件 {i}'.encode('utf-8')
            producer.send_async(message, callback=send_callback)
            
            if i % 200 == 0:
                logger.info(f"已发送 {i} 条消息")
        
        # 等待所有异步消息发送完成
        producer.flush()
        
        logger.info("✅ 所有消息已发送完成")
        
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
