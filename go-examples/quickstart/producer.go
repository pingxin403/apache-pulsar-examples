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

	fmt.Println("🚀 开始 Producer 示例...\n")

	// 创建 Producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "persistent://public/default/go-quickstart-topic",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	fmt.Println("✅ Producer 已创建\n")

	// 发送消息
	fmt.Println("📤 发送消息:")
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Go 消息 %d", i)
		msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(message),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 发送: %s (消息ID: %v)\n", message, msgID)
	}

	fmt.Println("\n✅ Producer 示例执行完成")
	fmt.Println("💡 提示: 运行 consumer.go 接收消息")
}
