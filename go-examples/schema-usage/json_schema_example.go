package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// User 用户数据结构
type User struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email"`
}

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := "persistent://public/default/go-json-schema-topic"
	fmt.Println("🚀 开始 JSON Schema 示例...\n")

	// JSON Schema 定义
	jsonSchemaDef := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "email", "type": "string"}
		]
	}`

	properties := map[string]string{
		"__alwaysAllowNull": "true",
	}

	jsonSchema := pulsar.NewJSONSchema(jsonSchemaDef, properties)

	// 创建带 Schema 的 Producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:  topic,
		Schema: jsonSchema,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// 发送结构化消息
	users := []User{
		{Name: "张三", Age: 28, Email: "zhangsan@example.com"},
		{Name: "李四", Age: 32, Email: "lisi@example.com"},
		{Name: "王五", Age: 25, Email: "wangwu@example.com"},
	}

	fmt.Println("📤 发送 JSON Schema 消息:")
	for _, user := range users {
		data, _ := json.Marshal(user)
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Value: &user,
		})
		if err != nil {
			// 回退到原始字节发送
			_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: data,
			})
			if err != nil {
				log.Printf("   ⚠️ 发送失败: %v", err)
				continue
			}
		}
		fmt.Printf("   ✅ 发送: %s (age=%d)\n", user.Name, user.Age)
	}

	// 创建带 Schema 的 Consumer
	fmt.Println("\n📥 创建 JSON Schema Consumer...")
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "go-json-schema-sub",
		Schema:           jsonSchema,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Println("📩 接收消息:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < len(users); i++ {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			break
		}
		var user User
		if err := json.Unmarshal(msg.Payload(), &user); err != nil {
			fmt.Printf("   ⚠️ 反序列化失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 接收: %s (age=%d, email=%s)\n", user.Name, user.Age, user.Email)
		}
		consumer.Ack(msg)
	}

	fmt.Println("\n✅ JSON Schema 示例执行完成")
	fmt.Println("💡 Schema 确保 Producer 和 Consumer 的数据格式一致")
}
