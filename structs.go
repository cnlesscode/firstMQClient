package firstMQClient

import (
	"net"

	"github.com/cnlesscode/firstKV"
)

// MQ 消息结构体
type Message struct {
	Action        int
	Topic         string
	ConsumerGroup string
	Data          []byte
}

// 响应消息结构体
type ResponseMessage struct {
	ErrCode int
	Data    string
}

// TCP 连接对象结构体
type TCPConnection struct {
	MapKey string
	Conn   net.Conn // TCP 连接指针
	Addr   string   // TCP 服务 Address
	Status bool     // 状态
}

// 连接池结构体
type MQConnectionPool struct {
	Key                  string
	FirstKVAddr          string // FirstKV 地址
	Addresses            map[string]firstKV.Item
	AddressesLen         int                            // TCP 服务地址数量
	Channels             map[string]chan *TCPConnection // 对应各服务器的连接池
	Channel              chan *TCPConnection            // 总接池
	Capacity             int                            // 连接池总容量
	ConnNumber           map[string]int                 // 对应某个服务器应建立的连接数量
	ConnDifferenceNumber map[string]int                 // 连接池数量与实际数量差值
	ErrorMessage         chan []byte                    // 错误消息临时记录通道
	InitTimes            int                            // 节点巡查次数
	Status               bool                           // 状态
}
