package firstMQClient

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	"github.com/cnlesscode/firstKV"
)

// 已有连接池 map
// 利用 map 键实现单利
var MQPoolMap map[string]*MQConnectionPool = make(map[string]*MQConnectionPool)

// 建立连接池 :
// firstKVAddr  FirstKV 服务地址,
// capacity 连接池容量,
// purpose 用途，由于单例区分
func New(firstKVAddr string, capacity int, purpose string) (*MQConnectionPool, error) {
	mapKey := firstKVAddr + purpose
	_, ok := MQPoolMap[mapKey]
	// 已有连接池直接返回
	if ok {
		return MQPoolMap[mapKey], nil
	}
	// 新建连接池
	MQPoolMap[mapKey] = &MQConnectionPool{
		Key:                  mapKey,
		FirstKVAddr:          firstKVAddr,
		Addresses:            nil,
		AddressesLen:         0,
		Channel:              make(chan *TCPConnection, capacity+1000),
		Channels:             make(map[string]chan *TCPConnection),
		Capacity:             capacity,
		ConnNumber:           make(map[string]int),
		ConnDifferenceNumber: make(map[string]int),
		ErrorMessage:         make(chan []byte, 1000000),
		InitTimes:            0,
		Status:               false,
	}

	// 1. 间隔30秒刷新一次服务器列表
	go func() {
		for {
			time.Sleep(time.Second * 30)
			MQPoolMap[mapKey].Init()
		}
	}()

	// 2. 监听错误消息并自动发送
	go func(mapKeyIn string) {
		for {
			select {
			case message := <-MQPoolMap[mapKeyIn].ErrorMessage:
				conn, err := MQPoolMap[mapKeyIn].GetAConnection()
				// 如果有错再放回错误连消息chi
				if err != nil {
					time.Sleep(time.Second)
					select {
					case MQPoolMap[mapKeyIn].ErrorMessage <- message:
					default:
					}
				} else {
					conn.SendBytes(message)
				}
			default:
				time.Sleep(time.Second * 3)
			}
		}
	}(mapKey)

	// 3. 初始化连接池
	err := MQPoolMap[mapKey].Init()
	if err != nil {
		return MQPoolMap[mapKey], err
	}

	// 心跳消息
	// go func() {
	// 	time.Sleep(time.Second * 3)
	// 	for {
	// 		MQPoolMap[mapKey].Send(Message{
	// 			Action: 6,
	// 		})
	// 		time.Sleep(time.Second)
	// 	}
	// }()

	return MQPoolMap[mapKey], err
}

func (m *MQConnectionPool) Init() error {
	// 整理服务地址
	err := m.GetMQServerAddresses()
	if err != nil {
		m.Status = false
		return errors.New("无可用服务 E10001")
	}
	addressesLen := len(m.Addresses)
	if addressesLen < 1 {
		m.Status = false
		return errors.New("无可用服务 E10002")
	}
	m.AddressesLen = addressesLen
	m.Status = true
	// 计算每个节点应该建立的连接数
	if m.Capacity < m.AddressesLen {
		m.Capacity = m.AddressesLen
	}
	// 首次启动
	if m.InitTimes == 0 {
		m.InitTimes++
		m.InitFirst()
		return nil
	} else {
		capacityForEveryServer := m.Capacity / m.AddressesLen
		// 遍历各个节点的连接池
		for channelKey := range m.Addresses {
			// 新的节点
			if _, ok := m.Channels[channelKey]; !ok {
				m.ConnNumber[channelKey] = capacityForEveryServer
				m.InitNewNode(channelKey)
			} else {
				// 已有节点
				m.ConnDifferenceNumber[channelKey] = capacityForEveryServer - m.ConnNumber[channelKey]
				m.ConnNumber[channelKey] = capacityForEveryServer
				if m.ConnDifferenceNumber[channelKey] > 0 {
					for i := 0; i < m.ConnDifferenceNumber[channelKey]; i++ {
						tcpConnection, _ := m.NewAClientForPool(channelKey)
						m.Channels[channelKey] <- tcpConnection
					}
					log.Println("✔ 新增连接 : ", channelKey, " 完成，新增数量 : ", m.ConnDifferenceNumber[channelKey])
					m.ConnDifferenceNumber[channelKey] = 0
				} else if m.ConnDifferenceNumber[channelKey] < 0 {
					log.Println("※ 需要减少连接 : ", channelKey, m.ConnDifferenceNumber[channelKey])
					for i := 0; i > m.ConnDifferenceNumber[channelKey]; i-- {
						tcpConnection := <-m.Channel
						if tcpConnection.Addr == m.Addresses[channelKey].Addr {
							if tcpConnection.Conn != nil {
								tcpConnection.Conn.Close()
							}
						} else {
							m.Channel <- tcpConnection
						}
					}
					m.ConnDifferenceNumber[channelKey] = 0
					log.Println("✔ 减少连接 : ", channelKey, " 完成")
				}
			}
		}
	}
	return nil
}

func (m *MQConnectionPool) InitFirst() {
	capacityForEveryServer := m.Capacity / m.AddressesLen
	// 创建对应各个节点的连接池
	for channelKey := range m.Addresses {
		m.ConnNumber[channelKey] = capacityForEveryServer
		m.InitNewNode(channelKey)
	}
}

// 初始化新节点服务器的连接
func (m *MQConnectionPool) InitNewNode(channelKey string) {
	m.Channels[channelKey] = make(chan *TCPConnection, m.ConnNumber[channelKey]+10)
	m.ConnDifferenceNumber[channelKey] = 0
	for i := 0; i < m.ConnNumber[channelKey]; i++ {
		tcpConnection, err := m.NewAClientForPool(channelKey)
		if err != nil {
			continue
		}
		m.Channels[channelKey] <- tcpConnection
	}

	// 监听错误连接并尝试修复
	go func() {
		var err error
		for {
			tcpConnection := <-m.Channels[channelKey]
			if !tcpConnection.Status {
				if tcpConnection.Conn != nil {
					tcpConnection.Conn.Close()
				}
				tcpConnection, err = m.NewAClientForPool(channelKey)
				if err == nil {
					m.Channel <- tcpConnection
				} else {
					// 将无法修复的坏连接给回自己的池子
					// 延迟1秒后再次尝试
					m.Channels[channelKey] <- tcpConnection
					time.Sleep(time.Second)
				}
			} else {
				m.Channel <- tcpConnection
			}
		}
	}()
}

// 获取一个连接
func (m *MQConnectionPool) GetAConnection() (*TCPConnection, error) {
	if !m.Status {
		return nil, errors.New("无法获取有效连接 E200100")
	}
	select {
	case tcpConnection := <-m.Channel:
		return tcpConnection, nil
	default:
		return nil, errors.New("无法获取有效连接 E200101")
	}
}

// 获取 MQ 服务列表
func (m *MQConnectionPool) GetMQServerAddresses() error {
	res, err := m.FirstKVSendMessage(firstKV.ReceiveMessage{
		Action: "get mqServers",
		Key:    "firstMQServers",
	})
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(res.Data), &m.Addresses)
	if err != nil {
		return err
	}
	// 验证 MQ 服务
	for _, mqServer := range m.Addresses {
		connIn, err := net.DialTimeout("tcp", mqServer.Addr, time.Second*3)
		// 验证失败将其移除
		if err != nil {
			message := firstKV.ReceiveMessage{
				Action: "remove mqServer",
				Key:    "firstMQServers",
				Data:   firstKV.FirstMQAddr{Addr: mqServer.Addr},
			}
			_, err := m.FirstKVSendMessage(message)
			if err != nil {
				log.Println("MQServer ", mqServer.Addr, " 验证失败, 已将其移除.")
			}
			delete(m.Addresses, mqServer.Addr)
		} else {
			connIn.Close()
		}
	}
	return nil
}
