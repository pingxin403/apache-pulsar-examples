#!/usr/bin/env python3
"""
订单事件 Producer - Key_Shared 示例
对应文章: 03-核心功能深度篇/01-Key_Shared订阅实战.md
"""

import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('persistent://public/default/order-events')

order_id = "order-12345"

# 发送同一订单的多个事件
producer.send(
    content=b"订单创建",
    partition_key=order_id  # 关键：指定 Key
)

producer.send(
    content=b"订单支付",
    partition_key=order_id
)

producer.send(
    content=b"订单发货",
    partition_key=order_id
)

print(f"✅ 订单事件已发送，Key: {order_id}")

producer.close()
client.close()
