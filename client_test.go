package firstMQClient

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

var addr string = "192.168.31.100:8803"

// 创建话题
// 测试命令 : go test -v -run=TestCreateATopic
func TestCreateATopic(t *testing.T) {
	// 创建话题
	mqPool, err := New(addr, 2, 5, "CreateTopic")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{Action: 3, Topic: "test"})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf(response.Data)
	}
}

// 生产消息 - 单条
// 测试命令 : go test -v -run=TestProductAMessage
func TestProductAMessage(t *testing.T) {
	mqPool, err := New(addr, 1, 5, "ProductAMessage")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{
		Action: 1,
		Topic:  "test",
		Data:   "a test message 2 ...",
	})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf(response.Data)
	}
}

// 生产消息 - 并发多条
// 测试命令 : go test -v -run=TestProductMessages
func TestProductMessages(t *testing.T) {
	mqPool, err := New(addr, 2000, 5000, "ProductMessages")
	if err != nil {
		panic(err.Error())
	}
	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Printf("协程数 : %v\n", runtime.NumGoroutine())
		}
	}()
	// 循环10次, 每次2w条消息 = 20w条消息
	for i := 0; i < 10; i++ {
		// 开始1w个协程，并发写入
		for i := 0; i < 20000; i++ {
			go func() {
				mqPool.Send(Message{
					Action: 1,
					Topic:  "test",
					Data:   "a test message 2 ...",
				})
			}()
		}
	}
	// 死循环
	for {
		time.Sleep(time.Second * 5)
	}
}
