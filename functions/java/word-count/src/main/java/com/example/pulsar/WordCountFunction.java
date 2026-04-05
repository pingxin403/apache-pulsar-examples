package com.example.pulsar;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * Word Count Pulsar Function
 * 
 * 接收文本消息，统计单词数量，输出 "word:count" 格式的结果。
 * 使用 Pulsar Function State 存储累计计数。
 * 
 * 输入 Topic: persistent://public/default/sentences
 * 输出 Topic: persistent://public/default/word-counts
 */
public class WordCountFunction implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        if (input == null || input.trim().isEmpty()) {
            return null;
        }

        String[] words = input.toLowerCase().trim().split("\\s+");
        StringBuilder result = new StringBuilder();

        for (String word : words) {
            // 清理标点符号
            word = word.replaceAll("[^a-zA-Z0-9\\u4e00-\\u9fa5]", "");
            if (word.isEmpty()) {
                continue;
            }

            // 使用 State 存储累计计数
            String stateKey = "word_" + word;
            long currentCount = 0;

            // 获取当前计数
            try {
                byte[] stateValue = context.getState(stateKey);
                if (stateValue != null) {
                    currentCount = Long.parseLong(new String(stateValue));
                }
            } catch (Exception e) {
                // State 不存在，使用默认值 0
            }

            // 递增计数
            currentCount++;
            context.putState(stateKey, String.valueOf(currentCount).getBytes());

            if (result.length() > 0) {
                result.append(",");
            }
            result.append(word).append(":").append(currentCount);
        }

        // 记录处理指标
        context.recordMetric("words_processed", words.length);

        context.getLogger().info(String.format("处理文本: '%s' -> %s", input, result));
        return result.toString();
    }
}
