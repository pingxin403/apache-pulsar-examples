#!/usr/bin/env python3
"""
Pulsar Python Producer 快速入门示例
对应文章: 01-入门篇/01-5分钟上手Pulsar.md
"""

import pulsar

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client('pulsar://localhost:6650')
    
    # 创建生产者
    producer = client.create_producer('my-python-topic')
    
    try:
        # 发送消息
        producer.send(b"Hello from Python!")
        print("✅ 消息已发送")
    finally:
        # 关闭资源
        producer.close()
        client.close()

if __name__ == "__main__":
    main()
