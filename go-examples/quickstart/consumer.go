package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	// 创建 Pulsar 客户端
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("🚀 开始 Consumer 示例...\n")

	// 创建 Consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "persistent://public/default/go-quickstart-topic",
		SubscriptionName: "go-quickstart-subscription",
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Println("✅ Consumer 已创建")
	fmt.Println("💡 等待接收消息（按 Ctrl+C 退出）...\n")

	// 接收消息
	fmt.Println("📩 接收消息:")
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   ✅ 接收: %s\n", string(msg.Payload()))
		fmt.Printf("      消息ID: %v\n", msg.ID())

		// 确认消息
		consumer.Ack(msg)
	}

	fmt.Println("\n✅ Consumer 示例执行完成")
}
