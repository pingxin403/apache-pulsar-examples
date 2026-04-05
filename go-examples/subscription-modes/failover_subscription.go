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

	topic := "persistent://public/default/go-failover-topic"
	fmt.Println("🚀 开始 Failover 订阅模式示例...\n")

	// 创建 Producer 并发送消息
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	fmt.Println("📤 发送测试消息:")
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("Failover 消息 %d", i)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 发送: %s\n", msg)
	}

	// 创建两个 Failover Consumer（主备模式）
	fmt.Println("\n📥 创建 Failover Consumer（主备模式）...")
	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-failover-sub",
		Type:             pulsar.Failover,
		Name:             "consumer-primary",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer1.Close()
	fmt.Println("   ✅ Consumer-Primary 已创建")

	consumer2, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-failover-sub",
		Type:             pulsar.Failover,
		Name:             "consumer-standby",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer2.Close()
	fmt.Println("   ✅ Consumer-Standby 已创建")
	fmt.Println("   ℹ️  只有 Primary 接收消息，Standby 待命\n")

	// Primary 接收消息
	fmt.Println("📩 Primary Consumer 接收消息:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		msg, err := consumer1.Receive(ctx)
		if err != nil {
			break
		}
		fmt.Printf("   ✅ [Primary] 接收: %s\n", string(msg.Payload()))
		consumer1.Ack(msg)
	}

	fmt.Println("\n✅ Failover 订阅模式示例执行完成")
	fmt.Println("💡 特点: 主 Consumer 故障时，备 Consumer 自动接管")
}
