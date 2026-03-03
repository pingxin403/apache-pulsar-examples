package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Individual ACK 示例
// 对应文章: 01-入门篇/07-Consumer-ACK机制.md
//
// Individual ACK（单条确认）是最常用的确认方式，适合 Shared 订阅模式。
// 每条消息单独确认，失败的消息不影响其他消息的处理。

// processMessage 处理消息的业务逻辑
func processMessage(content string) error {
	if content == "error" {
		return fmt.Errorf("业务处理失败")
	}
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

	// 创建 Consumer，使用 Shared 订阅模式
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "persistent://public/default/individual-ack-topic",
		SubscriptionName: "individual-ack-subscription",
		Type:             pulsar.Shared, // Shared 订阅，支持多个 Consumer 并行消费
	})
	if err != nil {
		log.Fatalf("❌ 创建 Consumer 失败: %v\n", err)
		return
	}
	defer consumer.Close()

	fmt.Println("🚀 Consumer 已启动，使用 Individual ACK 模式")
	fmt.Println("📌 订阅类型: Shared（支持多个 Consumer 并行消费）")
	fmt.Println("📌 Individual ACK 会单独确认每条消息\n")

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
			// 处理失败，不确认消息
			fmt.Printf("❌ 消息处理失败: %v\n", err)
			fmt.Println("⚠️ 消息未确认，将被重新投递\n")
			// 注意：不调用 Ack()，消息会在 ACK 超时后重新投递
		} else {
			// 处理成功，单独确认这条消息
			consumer.Ack(msg)
			fmt.Printf("✅ 消息已确认: %v\n\n", msg.ID())
		}
	}

	fmt.Println("\n📊 Individual ACK 特点:")
	fmt.Println("✅ 精确控制：可以单独确认或拒绝每条消息")
	fmt.Println("✅ 灵活性高：适合 Shared 订阅模式")
	fmt.Println("✅ 容错性好：失败的消息不影响其他消息")
	fmt.Println("❌ 网络开销：每条消息都需要一次 ACK 请求")

	fmt.Println("\n🔚 Consumer 已关闭")
}
