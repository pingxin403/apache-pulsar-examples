#!/usr/bin/env python3
"""
批量发送示例
对应文章: 01-入门篇/06-Producer发送模式.md

批量发送适用于高吞吐量、低延迟要求的场景，如 IoT 设备数据上报、日志收集等。
优点：降低网络开销，提高吞吐量
缺点：增加了消息延迟（需要等待批次填满或超时）
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
    """主函数：演示批量发送消息"""
    client: Optional[pulsar.Client] = None
    producer: Optional[pulsar.Producer] = None
    
    try:
        # 创建 Pulsar 客户端
        client = pulsar.Client('pulsar://localhost:6650')
        
        # 创建 Producer，启用批量发送
        producer = client.create_producer(
            topic='persistent://public/default/iot-sensor-data',
            batching_enabled=True,  # 启用批量发送
            batching_max_messages=100,  # 每批最多 100 条消息
            batching_max_publish_delay_ms=10  # 最多等待 10ms
        )
        
        logger.info("开始批量发送消息...")
        logger.info("批量配置 - 最大消息数: 100, 最大延迟: 10ms")
        
        # 发送 1000 条小消息
        for i in range(1000):
            message = f'传感器数据 {i}'.encode('utf-8')
            producer.send_async(message, callback=None)
            
            if i % 200 == 0:
                logger.info(f"已发送 {i} 条消息")
        
        # 等待所有批次发送完成
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
