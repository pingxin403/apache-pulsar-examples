#!/usr/bin/env python3
"""
情感分析 Pulsar Function

接收文本消息，进行简单的情感分析，输出情感标签和分数。
使用基于关键词的简单情感分析（生产环境建议使用 ML 模型）。

输入 Topic: persistent://public/default/reviews
输出 Topic: persistent://public/default/sentiment-results
"""

from pulsar import Function
import json
import re


# 简单的情感词典
POSITIVE_WORDS = {
    'good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
    'love', 'like', 'best', 'happy', 'perfect', 'awesome', 'nice',
    '好', '棒', '优秀', '喜欢', '满意', '推荐', '不错', '赞'
}

NEGATIVE_WORDS = {
    'bad', 'terrible', 'awful', 'horrible', 'hate', 'worst', 'poor',
    'disappointing', 'ugly', 'broken', 'slow', 'expensive', 'waste',
    '差', '烂', '垃圾', '失望', '难用', '退货', '坑', '糟糕'
}


class SentimentFunction(Function):
    """情感分析 Function"""

    def process(self, input_text, context):
        if not input_text:
            return None

        text = input_text.lower().strip()
        words = set(re.findall(r'[\w\u4e00-\u9fa5]+', text))

        positive_count = len(words & POSITIVE_WORDS)
        negative_count = len(words & NEGATIVE_WORDS)
        total = positive_count + negative_count

        if total == 0:
            sentiment = 'neutral'
            score = 0.0
        elif positive_count > negative_count:
            sentiment = 'positive'
            score = positive_count / total
        else:
            sentiment = 'negative'
            score = -(negative_count / total)

        result = {
            'text': input_text[:100],
            'sentiment': sentiment,
            'score': round(score, 2),
            'positive_words': list(words & POSITIVE_WORDS),
            'negative_words': list(words & NEGATIVE_WORDS)
        }

        # 记录指标
        context.record_metric('messages_processed', 1)
        context.record_metric(f'sentiment_{sentiment}', 1)

        context.get_logger().info(
            f"分析完成: '{input_text[:50]}...' -> {sentiment} ({score:.2f})"
        )

        return json.dumps(result, ensure_ascii=False)
