package firstMQClient

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 测试命令 : go test -v -run=TestMain
func TestMain(t *testing.T) {
	addr := "192.168.31.100:8803"

	// 创建话题
	// CreateTopic(addr, "topic01")

	// 生产消息
	// ProductMessage(addr, "topic01")

	// // 创建消费者组
	// CreateConsumerGroup(addr, "default", "c002")
	// // 消费消息
	ConsumeMessage(addr, "topic01", "default")
}

// 创建话题
func CreateTopic(addr string, topicName string) {
	mqPool, err := New(addr, 1, 5, "CreateTopic")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{
		Action: 3,
		Topic:  topicName})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf(response.Data)
	}
}

// 创建消费者组
func CreateConsumerGroup(addr string, topicName string, consumeGroup string) {
	mqPool, err := New(addr, 1, 5, "CreateConsumerGroup")
	if err != nil {
		panic(err.Error())
	}
	_, err = mqPool.Send(Message{
		Action: 7, Topic: topicName, ConsumerGroup: consumeGroup})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("消费者组创建成功")
	}
}

// 消费消息
func ConsumeMessage(addr, topicName, consumerGroup string) {
	// 连接 MQ
	mqPool, err := New(addr, 1000, 5, "consume")
	if err != nil {
		panic(err.Error())
	}
	for {
		var wg sync.WaitGroup
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := mqPool.Send(Message{
					Action:        2,
					Topic:         topicName,
					ConsumerGroup: consumerGroup,
					Data:          nil,
				})
				if err != nil {
					fmt.Printf("err: %v\n", err)
				} else {
					fmt.Printf("res: %v\n", res.Data)
				}
			}()
		}
		wg.Wait()
	}
}

// 生产消息
func ProductMessage(addr string, topicName string) {
	mqPool, err := New(addr, 1000, 5, "product")
	if err != nil {
		panic(err.Error())
	}
	timeStart := time.Now().UnixMilli()
	// step1:
	for i := 1; i <= 10; i++ {
		var wg sync.WaitGroup
		for ii := 0; ii < 1000; ii++ {
			wg.Add(1)
			go func(step int) {
				defer wg.Done()
				_, err := mqPool.Send(Message{
					Action: 1,
					Topic:  topicName,
					Data:   strconv.Itoa(step) + " : message",
				})
				if err != nil {
					fmt.Printf("err: %v\n", err)
				}
			}(ii * i)
		}
		wg.Wait()
	}
	log.Println("耗时: ", time.Now().UnixMilli()-timeStart, "毫秒")
	time.Sleep(time.Second * 10)
}
