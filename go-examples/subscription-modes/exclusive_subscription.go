package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := "persistent://public/default/go-exclusive-topic"
	fmt.Println("🚀 开始 Exclusive 订阅模式示例...\n")

	// 创建 Producer 并发送消息
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	fmt.Println("📤 发送测试消息:")
	for i := 1; i <= 5; i++ {
		msg := fmt.Sprintf("Exclusive 消息 %d", i)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 发送: %s\n", msg)
	}

	// 创建 Exclusive Consumer
	fmt.Println("\n📥 创建 Exclusive Consumer...")
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-exclusive-sub",
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	fmt.Println("   ✅ Consumer 已创建（Exclusive 模式）")
	fmt.Println("   ℹ️  只有一个 Consumer 可以接收消息\n")

	// 接收消息
	fmt.Println("📩 接收消息:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 接收: %s\n", string(msg.Payload()))
		consumer.Ack(msg)
	}

	fmt.Println("\n✅ Exclusive 订阅模式示例执行完成")
	fmt.Println("💡 特点: 只有一个 Consumer 可以消费，保证消息顺序")
}
