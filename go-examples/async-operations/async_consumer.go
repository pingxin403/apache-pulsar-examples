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

	topic := "persistent://public/default/go-async-topic"
	fmt.Println("🚀 开始异步 Consumer 示例...\n")

	// 先发送一些消息
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("📤 发送测试消息:")
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("异步消费消息 %d", i)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ✅ 发送: %s\n", msg)
	}
	producer.Close()

	// 使用 MessageChannel 实现异步消费
	fmt.Println("\n📥 创建异步 Consumer（使用 MessageChannel）...")
	msgChan := make(chan pulsar.ConsumerMessage, 100)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-async-consumer-sub",
		Type:             pulsar.Shared,
		MessageChannel:   msgChan,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	fmt.Println("   ✅ Consumer 已创建\n")

	// 多个 goroutine 并发处理消息
	fmt.Println("📩 并发处理消息（2 个 worker）:")
	var wg sync.WaitGroup
	received := 0
	mu := sync.Mutex{}

	for w := 1; w <= 2; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case msg := <-msgChan:
					mu.Lock()
					received++
					count := received
					mu.Unlock()
					fmt.Printf("   📩 [Worker-%d] 接收: %s\n", workerID, string(msg.Payload()))
					// 模拟异步处理
					time.Sleep(50 * time.Millisecond)
					consumer.Ack(msg)
					if count >= 10 {
						return
					}
				case <-time.After(5 * time.Second):
					return
				}
			}
		}(w)
	}

	wg.Wait()

	fmt.Println("\n✅ 异步 Consumer 示例执行完成")
	fmt.Println("💡 使用 MessageChannel + goroutine 实现高并发消费")
}
