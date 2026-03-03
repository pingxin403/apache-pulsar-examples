package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

// 同步发送示例
// 对应文章: 01-入门篇/06-Producer发送模式.md
//
// 同步发送适用于需要确保消息可靠送达的场景，如支付订单确认、重要通知等。
// 优点：可靠性高，能立即知道发送结果
// 缺点：吞吐量较低，每次发送都需要等待响应

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
		Topic: "persistent://public/default/payment-orders",
	})
	if err != nil {
		log.Fatalf("❌ 创建 Producer 失败: %v\n", err)
		return
	}
	defer producer.Close()

	fmt.Println("开始同步发送消息...")

	// 同步发送消息
	ctx := context.Background()
	msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("订单 12345 支付成功"),
	})
	if err != nil {
		log.Fatalf("❌ 发送消息失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 消息发送成功，MessageId: %v\n", msgID)

	// 再发送几条消息
	for i := 1; i <= 5; i++ {
		orderID := 12345 + i
		message := fmt.Sprintf("订单 %d 支付成功", orderID)
		msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(message),
		})
		if err != nil {
			log.Printf("❌ 订单 %d 消息发送失败: %v\n", orderID, err)
			continue
		}
		fmt.Printf("✅ 订单 %d 消息发送成功，MessageId: %v\n", orderID, msgID)
	}

	fmt.Println("程序执行完成")
}
