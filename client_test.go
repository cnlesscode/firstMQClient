package firstMQClient

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cnlesscode/firstKV"
)

var addr string = "192.168.31.100:8803"

// 创建话题
// go test -v -run=TestCreateATopic
func TestCreateATopic(t *testing.T) {
	// 创建话题
	mqPool, err := New(addr, 2, "CreateTopic")
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
// go test -v -run=TestProductAMessage
func TestProductAMessage(t *testing.T) {
	mqPool, err := New(addr, 1, "ProductAMessage")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{
		Action: 1,
		Topic:  "test",
		Data:   []byte("a test message ..."),
	})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf(response.Data)
	}
	for {
		time.Sleep(time.Second * 5)
	}
}

// 生产消息 - 并发多条
// go test -v -run=TestProductMessages
func TestProductMessages(t *testing.T) {
	mqPool, err := New(addr, 1000, "ProductMessages")
	if err != nil {
		panic(err.Error())
	}
	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Printf("协程数 : %v\n", runtime.NumGoroutine())
		}
	}()
	// 循环批量生产消息
	for i := 0; i < 10; i++ {
		wg := sync.WaitGroup{}
		// 开始1w个协程，并发写入
		for ii := 1; ii <= 10000; ii++ {
			n := i*10000 + ii
			wg.Add(1)
			go func(iin int) {
				defer wg.Done()
				mqPool.Send(Message{
					Action: 1,
					Topic:  "test",
					Data:   []byte(strconv.Itoa(iin) + " test message ..."),
				})
			}(n)
		}
		wg.Wait()
		fmt.Printf("第%v次写入完成\n", i+1)
	}
	// 死循环
	for {
		time.Sleep(time.Second * 5)
	}
}

// go test -v -run=TestConsumeMessage
func TestConsumeMessage(t *testing.T) {
	mqPool, err := New(addr, 100, "ConsumeMessage")
	if err != nil {
		panic(err.Error())
	}
	mp := make(map[string]int, 0)
	step := 1
	for {
		response, err := mqPool.Send(Message{
			Action:        2,
			Topic:         "test",
			ConsumerGroup: "default",
		})
		if err != nil {
			fmt.Printf("err: %v\n", err)
			fmt.Printf("len(mp): %v\n", len(mp))
			time.Sleep(time.Second * 10)
		} else {
			fmt.Printf("step: %v\n", step)
			fmt.Printf("response.Data: %v\n", response.Data)
			mp[response.Data] = 1
			step++
		}
	}
}

// go test -v -run=TestCreateConsumeGroup
func TestCreateConsumeGroup(t *testing.T) {
	mqPool, err := New(addr, 1, "ConsumeMessage")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{
		Action:        7,
		Topic:         "test",
		ConsumerGroup: "default",
	})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf(response.Data)
	}
}

// go test -v -run=TestServerList
func TestServerList(t *testing.T) {
	mqPool, err := New(addr, 1, "ConsumeMessage")
	if err != nil {
		panic(err.Error())
	}
	response, err := mqPool.Send(Message{
		Action: 10,
	})
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("response.Data: %v\n", response.Data)
		list := firstKV.FirstMQAddrs{}
		err := json.Unmarshal([]byte(response.Data), &list)
		if err == nil {
			fmt.Printf("list: %v\n", list)
		} else {
			fmt.Printf("not ok")
		}
	}
}
