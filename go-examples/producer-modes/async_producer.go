package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// 异步发送示例
// 对应文章: 01-入门篇/06-Producer发送模式.md
//
// 异步发送适用于高吞吐量场景，如用户行为日志、实时监控数据等。
// 优点：吞吐量高，不阻塞主线程
// 缺点：需要处理回调，错误处理相对复杂

func main() {
	// 创建 Pulsar 客户端
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatalf("❌ 连接 Pulsar 失败: %v\n", err)
		log.Println("请确保 Pulsar 服务正在运行:")
		log.Println("  docker-compose -f docker-compose/docker-compose.yml up -d")
		return
	}
	defer client.Close()

	// 创建 Producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "persistent://public/default/user-behavior-logs",
	})
	if err != nil {
		log.Fatalf("❌ 创建 Producer 失败: %v\n", err)
		return
	}
	defer producer.Close()

	fmt.Println("开始异步发送消息...")

	// 使用 WaitGroup 等待所有异步发送完成
	var wg sync.WaitGroup
	successCount := 0
	failCount := 0
	var mu sync.Mutex

	ctx := context.Background()

	// 异步发送 1000 条消息
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		message := fmt.Sprintf("用户点击事件 %d", i)

		// 异步发送消息
		producer.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload: []byte(message),
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				failCount++
				log.Printf("❌ 消息发送失败: %v\n", err)
			} else {
				successCount++
			}
		})

		if i%200 == 0 {
			fmt.Printf("已发送 %d 条消息\n", i)
		}
	}

	// 等待所有异步消息发送完成
	wg.Wait()

	fmt.Printf("✅ 所有消息已发送完成\n")
	fmt.Printf("   成功: %d 条\n", successCount)
	fmt.Printf("   失败: %d 条\n", failCount)
	fmt.Println("程序执行完成")
}
