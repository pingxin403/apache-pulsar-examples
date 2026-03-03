package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Negative ACK 示例
// 对应文章: 01-入门篇/07-Consumer-ACK机制.md
//
// Negative ACK（否定确认）明确告知 Pulsar 消息处理失败，触发快速重试。
// 适合处理临时性错误（如网络超时、服务暂时不可用）。

// RetryableError 可重试的错误类型
type RetryableError struct {
	message string
}

func (e *RetryableError) Error() string {
	return e.message
}

// processMessage 处理消息的业务逻辑
func processMessage(content string) error {
	if content == "timeout" {
		return &RetryableError{message: "网络超时"}
	}
	if content == "invalid" {
		return fmt.Errorf("数据格式错误")
	}
	// 正常处理逻辑
	fmt.Printf("✅ 消息处理成功: %s\n", content)
	return nil
}

func main() {
	// 创建 Pulsar 客户端
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatalf("❌ 连接 Pulsar 失败: %v\n", err)
		log.Println("请确保 Pulsar 服务正在运行:")
		log.Println("  docker-compose -f ../../docker-compose/docker-compose.yml up -d")
		return
	}
	defer client.Close()

	// 创建 Consumer，配置 Negative ACK 重新投递延迟
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:               "persistent://public/default/negative-ack-topic",
		SubscriptionName:    "negative-ack-subscription",
		Type:                pulsar.Shared,
		NackRedeliveryDelay: 1 * time.Second, // 1 秒后重新投递
	})
	if err != nil {
		log.Fatalf("❌ 创建 Consumer 失败: %v\n", err)
		return
	}
	defer consumer.Close()

	fmt.Println("🚀 Consumer 已启动，使用 Negative ACK 模式")
	fmt.Println("📌 订阅类型: Shared")
	fmt.Println("📌 Negative ACK 会触发消息快速重新投递（1 秒延迟）\n")

	ctx := context.Background()
	messageCount := 0
	maxMessages := 10

	for messageCount < maxMessages {
		// 接收消息（阻塞等待）
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Printf("❌ 接收消息失败: %v\n", err)
			continue
		}

		messageCount++

		// 处理消息
		content := string(msg.Payload())
		fmt.Printf("📩 收到消息 #%d: %s\n", messageCount, content)

		err = processMessage(content)
		if err != nil {
			// 判断错误类型
			if _, ok := err.(*RetryableError); ok {
				// 可重试的错误，发送 Nack
				fmt.Printf("⚠️ 消息处理失败，将重新投递: %v\n", err)
				consumer.Nack(msg)
				fmt.Println("🔄 已发送 Negative ACK，消息将在 1 秒后重新投递\n")
			} else {
				// 不可重试的错误，确认消息（避免无限重试）
				fmt.Printf("❌ 消息无法处理，已确认: %v\n", err)
				consumer.Ack(msg)
				fmt.Println("💡 不可重试的错误，确认消息以避免无限重试\n")
			}
		} else {
			// 处理成功，确认
			consumer.Ack(msg)
			fmt.Println("✅ 消息已确认\n")
		}
	}

	fmt.Println("📊 Negative ACK 特点:")
	fmt.Println("✅ 快速重试：立即触发消息重新投递")
	fmt.Println("✅ 明确语义：区分'处理失败'和'不确认'")
	fmt.Println("✅ 可配置延迟：控制重试间隔")
	fmt.Println("❌ 需要判断：需要区分可重试和不可重试错误")

	fmt.Println("\n🔚 Consumer 已关闭")
}
