#!/usr/bin/env python3
"""
用户行为事件生产者
模拟生成页面浏览、点击、购买等用户行为事件
"""

import pulsar
import json
import random
import time
import os
from datetime import datetime

PULSAR_URL = os.getenv('PULSAR_URL', 'pulsar://localhost:6650')
TOPIC = 'persistent://public/default/user-events'
EVENTS_PER_SECOND = int(os.getenv('EVENTS_PER_SECOND', '5'))

# 模拟数据
USERS = [f'user_{i:04d}' for i in range(1, 101)]
PAGES = ['/home', '/products', '/product/detail', '/cart', '/checkout', '/payment', '/order/success']
EVENT_TYPES = ['page_view', 'click', 'add_to_cart', 'purchase', 'search']
DEVICES = ['mobile', 'desktop', 'tablet']
REGIONS = ['us-east', 'us-west', 'eu-west', 'ap-southeast', 'ap-northeast']


def generate_event():
    """生成一个随机用户行为事件"""
    user_id = random.choice(USERS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        'user_id': user_id,
        'event_type': event_type,
        'page': random.choice(PAGES),
        'device': random.choice(DEVICES),
        'region': random.choice(REGIONS),
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'session_id': f'sess_{random.randint(1000, 9999)}',
    }

    if event_type == 'purchase':
        event['amount'] = round(random.uniform(9.99, 999.99), 2)
        event['product_id'] = f'prod_{random.randint(1, 500)}'
    elif event_type == 'search':
        event['query'] = random.choice(['laptop', 'phone', 'headphones', 'camera', 'tablet'])

    return user_id, event


def main():
    print(f"🚀 启动用户行为事件生产者...")
    print(f"   Pulsar URL: {PULSAR_URL}")
    print(f"   Topic: {TOPIC}")
    print(f"   速率: {EVENTS_PER_SECOND} events/s\n")

    client = pulsar.Client(PULSAR_URL)
    producer = client.create_producer(TOPIC)

    count = 0
    try:
        while True:
            user_id, event = generate_event()
            producer.send(
                json.dumps(event).encode('utf-8'),
                partition_key=user_id
            )
            count += 1
            if count % 100 == 0:
                print(f"   📤 已发送 {count} 条事件")
            time.sleep(1.0 / EVENTS_PER_SECOND)
    except KeyboardInterrupt:
        print(f"\n✅ 共发送 {count} 条事件")
    finally:
        producer.close()
        client.close()


if __name__ == '__main__':
    main()
