package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Cumulative ACK 示例
// 对应文章: 01-入门篇/07-Consumer-ACK机制.md
//
// Cumulative ACK（累积确认）会确认当前消息及之前的所有消息，适合顺序处理场景。
// 注意：只能用于 Exclusive 或 Failover 订阅模式。

// processMessage 处理消息的业务逻辑
func processMessage(content string) {
	// 模拟消息处理
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("✅ 消息处理成功: %s\n", content)
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

	// 创建 Consumer，使用 Exclusive 订阅模式
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "persistent://public/default/cumulative-ack-topic",
		SubscriptionName: "cumulative-ack-subscription",
		Type:             pulsar.Exclusive, // 必须使用 Exclusive 或 Failover
	})
	if err != nil {
		log.Fatalf("❌ 创建 Consumer 失败: %v\n", err)
		return
	}
	defer consumer.Close()

	fmt.Println("🚀 Consumer 已启动，使用 Cumulative ACK 模式")
	fmt.Println("📌 订阅类型: Exclusive（确保消息顺序处理）")
	fmt.Println("📌 Cumulative ACK 会确认当前消息及之前的所有消息\n")

	ctx := context.Background()
	messageCount := 0
	maxMessages := 10
	batchSize := 3 // 每处理 3 条消息，执行一次 Cumulative ACK

	var lastMsg pulsar.Message

	for messageCount < maxMessages {
		// 接收消息（阻塞等待）
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Printf("❌ 接收消息失败: %v\n", err)
			continue
		}

		messageCount++
		lastMsg = msg

		// 处理消息
		content := string(msg.Payload())
		fmt.Printf("📩 收到消息 #%d: %s\n", messageCount, content)
		processMessage(content)

		// 每处理 batchSize 条消息，执行一次 Cumulative ACK
		if messageCount%batchSize == 0 {
			consumer.AckCumulative(msg)
			fmt.Printf("✅ Cumulative ACK: 已确认消息 #1 到 #%d\n", messageCount)
			fmt.Printf("💡 网络开销: 1 次 ACK 请求确认了 %d 条消息\n\n", batchSize)
		} else {
			fmt.Println("⏳ 暂不确认，等待批量确认\n")
		}
	}

	// 确认剩余的消息
	if messageCount%batchSize != 0 {
		consumer.AckCumulative(lastMsg)
		fmt.Println("✅ Cumulative ACK: 已确认剩余消息\n")
	}

	fmt.Println("📊 性能对比:")
	fmt.Printf("Individual ACK: %d 条消息 = %d 次网络请求\n", maxMessages, maxMessages)
	ackCount := (maxMessages / batchSize)
	if maxMessages%batchSize != 0 {
		ackCount++
	}
	fmt.Printf("Cumulative ACK: %d 条消息 = %d 次网络请求（每 %d 条确认一次）\n", maxMessages, ackCount, batchSize)
	reduction := (1 - float64(ackCount)/float64(maxMessages)) * 100
	fmt.Printf("网络开销降低: %.0f%%\n", reduction)

	fmt.Println("\n📊 Cumulative ACK 特点:")
	fmt.Println("✅ 高效率：减少网络开销（批量确认）")
	fmt.Println("✅ 简单易用：适合顺序处理场景")
	fmt.Println("❌ 仅限 Exclusive/Failover：不支持 Shared 订阅")
	fmt.Println("❌ 容错性差：某条消息失败会阻塞后续消息")

	fmt.Println("\n🔚 Consumer 已关闭")
}
