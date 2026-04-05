package main

import (
	"context"
	"fmt"
	"log"
	"sync"
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

	topic := "persistent://public/default/go-key-shared-topic"
	fmt.Println("🚀 开始 Key_Shared 订阅模式示例...\n")

	// 创建 Producer 并发送带 Key 的消息
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}

	keys := []string{"order-001", "order-002", "order-003"}
	fmt.Println("📤 发送带 Key 的消息:")
	for round := 1; round <= 3; round++ {
		for _, key := range keys {
			msg := fmt.Sprintf("%s-事件-%d", key, round)
			_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
				Key:     key,
				Payload: []byte(msg),
			})
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("   ✅ 发送: Key=%s, Payload=%s\n", key, msg)
		}
	}
	producer.Close()

	// 创建两个 Key_Shared Consumer
	fmt.Println("\n📥 创建 Key_Shared Consumer...")
	var wg sync.WaitGroup

	for idx := 1; idx <= 2; idx++ {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: "go-key-shared-sub",
			Type:             pulsar.KeyShared,
			Name:             fmt.Sprintf("consumer-%d", idx),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ Consumer-%d 已创建\n", idx)

		wg.Add(1)
		go func(c pulsar.Consumer, id int) {
			defer wg.Done()
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				msg, err := c.Receive(ctx)
				if err != nil {
					return
				}
				fmt.Printf("   📩 [Consumer-%d] Key=%s, Payload=%s\n",
					id, msg.Key(), string(msg.Payload()))
				c.Ack(msg)
			}
		}(consumer, idx)
	}

	fmt.Println("   ℹ️  相同 Key 的消息始终路由到同一个 Consumer\n")
	wg.Wait()

	fmt.Println("\n✅ Key_Shared 订阅模式示例执行完成")
	fmt.Println("💡 特点: 按 Key 分组顺序消费，兼顾并行度和顺序性")
}
