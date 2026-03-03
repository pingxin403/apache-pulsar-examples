#!/usr/bin/env python3
"""
Pulsar Python Consumer 快速入门示例
对应文章: 01-入门篇/01-5分钟上手Pulsar.md
"""

import pulsar

def main():
    # 创建 Pulsar 客户端
    client = pulsar.Client('pulsar://localhost:6650')
    
    # 创建消费者
    consumer = client.subscribe('my-python-topic', 'python-sub')
    
    try:
        # 接收消息
        msg = consumer.receive()
        print("📩 收到消息:", msg.data().decode('utf-8'))
        
        # 确认消息
        consumer.acknowledge(msg)
    finally:
        # 关闭资源
        consumer.close()
        client.close()

if __name__ == "__main__":
    main()
