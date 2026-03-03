#!/usr/bin/env python3
"""
订单事件 Consumer - Key_Shared 示例
对应文章: 03-核心功能深度篇/01-Key_Shared订阅实战.md
"""

import pulsar

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(
    topic='persistent://public/default/order-events',
    subscription_name='order-processing',
    subscription_type=pulsar.SubscriptionType.KeyShared  # 关键：Key_Shared 模式
)

print("🔄 Consumer 已启动，等待消息...")

while True:
    msg = consumer.receive()
    
    print(f"📩 收到消息 [Key: {msg.partition_key()}] [Value: {msg.data().decode('utf-8')}]")
    
    consumer.acknowledge(msg)

consumer.close()
client.close()
