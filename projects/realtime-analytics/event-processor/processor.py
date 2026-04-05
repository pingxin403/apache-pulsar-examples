#!/usr/bin/env python3
"""
用户行为事件处理器
实时聚合用户行为数据，输出统计结果
"""

import pulsar
import json
import os
from collections import defaultdict
from datetime import datetime

PULSAR_URL = os.getenv('PULSAR_URL', 'pulsar://localhost:6650')
INPUT_TOPIC = 'persistent://public/default/user-events'
OUTPUT_TOPIC = 'persistent://public/default/analytics-results'


class EventAggregator:
    """实时事件聚合器"""

    def __init__(self):
        self.event_counts = defaultdict(int)
        self.page_views = defaultdict(int)
        self.revenue = 0.0
        self.purchase_count = 0
        self.active_users = set()
        self.window_start = datetime.utcnow()

    def process(self, event):
        self.event_counts[event['event_type']] += 1
        self.page_views[event['page']] += 1
        self.active_users.add(event['user_id'])

        if event['event_type'] == 'purchase':
            self.revenue += event.get('amount', 0)
            self.purchase_count += 1

    def get_summary(self):
        return {
            'window_start': self.window_start.isoformat() + 'Z',
            'window_end': datetime.utcnow().isoformat() + 'Z',
            'total_events': sum(self.event_counts.values()),
            'event_breakdown': dict(self.event_counts),
            'top_pages': dict(sorted(self.page_views.items(), key=lambda x: -x[1])[:5]),
            'active_users': len(self.active_users),
            'revenue': round(self.revenue, 2),
            'purchases': self.purchase_count,
        }

    def reset(self):
        self.__init__()


def main():
    print(f"🚀 启动事件处理器...")
    print(f"   输入: {INPUT_TOPIC}")
    print(f"   输出: {OUTPUT_TOPIC}\n")

    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        INPUT_TOPIC,
        subscription_name='analytics-processor',
        consumer_type=pulsar.ConsumerType.Shared
    )
    producer = client.create_producer(OUTPUT_TOPIC)

    aggregator = EventAggregator()
    batch_size = 100
    count = 0

    try:
        while True:
            msg = consumer.receive(timeout_millis=5000)
            event = json.loads(msg.data().decode('utf-8'))
            aggregator.process(event)
            consumer.acknowledge(msg)
            count += 1

            # 每 batch_size 条消息输出一次聚合结果
            if count % batch_size == 0:
                summary = aggregator.get_summary()
                producer.send(json.dumps(summary).encode('utf-8'))
                print(f"   📊 聚合结果: {summary['total_events']} 事件, "
                      f"{summary['active_users']} 活跃用户, "
                      f"收入 ${summary['revenue']}")
                aggregator.reset()

    except Exception as e:
        if count > 0:
            summary = aggregator.get_summary()
            producer.send(json.dumps(summary).encode('utf-8'))
            print(f"   📊 最终聚合: {summary['total_events']} 事件")
    finally:
        consumer.close()
        producer.close()
        client.close()


if __name__ == '__main__':
    main()
