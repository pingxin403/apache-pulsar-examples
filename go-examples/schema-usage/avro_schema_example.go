package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Order 订单数据结构
type Order struct {
	OrderID string  `json:"order_id" avro:"order_id"`
	Product string  `json:"product" avro:"product"`
	Amount  float64 `json:"amount" avro:"amount"`
	Status  string  `json:"status" avro:"status"`
}

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := "persistent://public/default/go-avro-schema-topic"
	fmt.Println("🚀 开始 Avro Schema 示例...\n")

	// Avro Schema 定义
	avroSchemaDef := `{
		"type": "record",
		"name": "Order",
		"namespace": "com.example.pulsar",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "product", "type": "string"},
			{"name": "amount", "type": "double"},
			{"name": "status", "type": "string"}
		]
	}`

	properties := map[string]string{
		"__alwaysAllowNull": "true",
	}

	avroSchema := pulsar.NewAvroSchema(avroSchemaDef, properties)

	// 创建带 Avro Schema 的 Producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:  topic,
		Schema: avroSchema,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// 发送订单消息
	orders := []Order{
		{OrderID: "ORD-001", Product: "MacBook Pro", Amount: 12999.00, Status: "created"},
		{OrderID: "ORD-002", Product: "iPhone 15", Amount: 7999.00, Status: "paid"},
		{OrderID: "ORD-003", Product: "AirPods Pro", Amount: 1899.00, Status: "shipped"},
	}

	fmt.Println("📤 发送 Avro Schema 消息:")
	for _, order := range orders {
		data, _ := json.Marshal(order)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: data,
		})
		if err != nil {
			log.Printf("   ⚠️ 发送失败: %v", err)
			continue
		}
		fmt.Printf("   ✅ 发送: OrderID=%s, Product=%s, Amount=%.2f\n",
			order.OrderID, order.Product, order.Amount)
	}

	// 创建带 Avro Schema 的 Consumer
	fmt.Println("\n📥 创建 Avro Schema Consumer...")
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-avro-schema-sub",
		Schema:           avroSchema,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Println("📩 接收消息:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < len(orders); i++ {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			break
		}
		var order Order
		if err := json.Unmarshal(msg.Payload(), &order); err != nil {
			fmt.Printf("   ⚠️ 反序列化失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 接收: OrderID=%s, Product=%s, Status=%s\n",
				order.OrderID, order.Product, order.Status)
		}
		consumer.Ack(msg)
	}

	fmt.Println("\n✅ Avro Schema 示例执行完成")
	fmt.Println("💡 Avro Schema 支持 Schema 演进（向前/向后兼容）")
}
