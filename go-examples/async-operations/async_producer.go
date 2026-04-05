package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	fmt.Println("🚀 开始异步 Producer 示例...\n")

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		BatchingMaxPublishDelay: 10 * time.Millisecond,
		SendTimeout:             30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// 异步发送消息
	messageCount := 20
	var successCount int64
	var failCount int64
	var wg sync.WaitGroup

	fmt.Printf("📤 异步发送 %d 条消息:\n", messageCount)
	start := time.Now()

	for i := 1; i <= messageCount; i++ {
		wg.Add(1)
		msg := fmt.Sprintf("异步消息 %d", i)
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(msg),
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			defer wg.Done()
			if err != nil {
				atomic.AddInt64(&failCount, 1)
				fmt.Printf("   ❌ 发送失败: %v\n", err)
			} else {
				atomic.AddInt64(&successCount, 1)
			}
		})
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n📊 发送统计:\n")
	fmt.Printf("   ✅ 成功: %d 条\n", successCount)
	fmt.Printf("   ❌ 失败: %d 条\n", failCount)
	fmt.Printf("   ⏱️  耗时: %v\n", elapsed)
	fmt.Printf("   📈 吞吐: %.0f msg/s\n", float64(successCount)/elapsed.Seconds())

	fmt.Println("\n✅ 异步 Producer 示例执行完成")
	fmt.Println("💡 异步发送不阻塞主线程，适合高吞吐场景")
}
