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

	topic := "persistent://public/default/go-shared-topic"
	fmt.Println("🚀 开始 Shared 订阅模式示例...\n")

	// 创建 Producer 并发送消息
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("📤 发送测试消息:")
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("Shared 消息 %d", i)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 发送: %s\n", msg)
	}
	producer.Close()

	// 创建两个 Shared Consumer（竞争消费）
	fmt.Println("\n📥 创建 Shared Consumer（竞争消费模式）...")
	var wg sync.WaitGroup

	for idx := 1; idx <= 2; idx++ {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: "go-shared-sub",
			Type:             pulsar.Shared,
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
				fmt.Printf("   📩 [Consumer-%d] 接收: %s\n", id, string(msg.Payload()))
				c.Ack(msg)
			}
		}(consumer, idx)
	}

	fmt.Println("   ℹ️  消息在多个 Consumer 间轮询分发\n")
	wg.Wait()

	fmt.Println("\n✅ Shared 订阅模式示例执行完成")
	fmt.Println("💡 特点: 多个 Consumer 竞争消费，无 Rebalance")
}
