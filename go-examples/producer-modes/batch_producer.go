package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// 批量发送示例
// 对应文章: 01-入门篇/06-Producer发送模式.md
//
// 批量发送适用于高吞吐量、低延迟要求的场景，如 IoT 设备数据上报、日志收集等。
// 优点：降低网络开销，提高吞吐量
// 缺点：增加了消息延迟（需要等待批次填满或超时）

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

	// 创建 Producer，启用批量发送
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   "persistent://public/default/iot-sensor-data",
		BatchingMaxMessages:     100,                   // 每批最多 100 条消息
		BatchingMaxPublishDelay: 10 * time.Millisecond, // 最多等待 10ms
	})
	if err != nil {
		log.Fatalf("❌ 创建 Producer 失败: %v\n", err)
		return
	}
	defer producer.Close()

	fmt.Println("开始批量发送消息...")
	fmt.Println("批量配置 - 最大消息数: 100, 最大延迟: 10ms")

	ctx := context.Background()

	// 发送 1000 条小消息
	for i := 0; i < 1000; i++ {
		message := fmt.Sprintf("传感器数据 %d", i)
		producer.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload: []byte(message),
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				log.Printf("❌ 消息发送失败: %v\n", err)
			}
		})

		if i%200 == 0 {
			fmt.Printf("已发送 %d 条消息\n", i)
		}
	}

	// 刷新所有批次，确保所有消息都发送完成
	err = producer.Flush()
	if err != nil {
		log.Fatalf("❌ 刷新批次失败: %v\n", err)
		return
	}

	fmt.Println("✅ 所有消息已发送完成")
	fmt.Println("程序执行完成")
}
