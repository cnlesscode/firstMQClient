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
		Key:          mapKey,
		FirstKVAddr:  firstKVAddr,
		Addresses:    nil,
		AddressesLen: 0,
		// 总连接池 [ 缓存管道 ]
		AllConnections: make(chan *MQConnection, capacity+1000),
		// 对应服务器节点的连接池
		MQConnections:        make(map[string]chan *MQConnection),
		Capacity:             capacity,
		ConnNumber:           make(map[string]int),
		ConnDifferenceNumber: make(map[string]int),
		ErrorMessage:         make(chan []byte, 1000000),
		InitTimes:            0,
		Status:               false,
	}

	// 初始化连接池
	err := MQPoolMap[mapKey].Init()
	if err != nil {
		return MQPoolMap[mapKey], err
	}

	// 监听错误消息并自动发送
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

	// 间隔30秒刷新一次服务器列表
	go func() {
		for {
			time.Sleep(time.Second * 30)
			MQPoolMap[mapKey].Init()
		}
	}()

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
			if _, ok := m.MQConnections[channelKey]; !ok {
				m.ConnNumber[channelKey] = capacityForEveryServer
				m.InitNewNode(channelKey)
			} else {
				// 已有节点
				m.ConnDifferenceNumber[channelKey] = capacityForEveryServer - m.ConnNumber[channelKey]
				m.ConnNumber[channelKey] = capacityForEveryServer
				if m.ConnDifferenceNumber[channelKey] > 0 {
					for i := 0; i < m.ConnDifferenceNumber[channelKey]; i++ {
						tcpConnection, _ := m.NewAClientForPool(channelKey)
						m.MQConnections[channelKey] <- tcpConnection
					}
					log.Println("✔ 新增连接 : ", channelKey, " 完成，新增数量 : ", m.ConnDifferenceNumber[channelKey])
					m.ConnDifferenceNumber[channelKey] = 0
				} else if m.ConnDifferenceNumber[channelKey] < 0 {
					log.Println("※ 需要减少连接 : ", channelKey, m.ConnDifferenceNumber[channelKey])
					for i := 0; i > m.ConnDifferenceNumber[channelKey]; i-- {
						tcpConnection := <-m.AllConnections
						if tcpConnection.Addr == channelKey {
							if tcpConnection.Conn != nil {
								tcpConnection.Conn.Close()
							}
						} else {
							m.AllConnections <- tcpConnection
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
	m.MQConnections[channelKey] = make(chan *MQConnection, m.ConnNumber[channelKey]+10)
	m.ConnDifferenceNumber[channelKey] = 0
	for i := 0; i < m.ConnNumber[channelKey]; i++ {
		tcpConnection, err := m.NewAClientForPool(channelKey)
		if err != nil {
			continue
		}
		m.MQConnections[channelKey] <- tcpConnection
	}

	// 从节点连接池中获取连接，填充进总连接池
	// 同时检查错误连接，并尝试修复
	go func() {
		var err error
		for {
			tcpConnection := <-m.MQConnections[channelKey]
			if !tcpConnection.Status {
				if tcpConnection.Conn != nil {
					tcpConnection.Conn.Close()
				}
				tcpConnection, err = m.NewAClientForPool(channelKey)
				if err == nil {
					m.AllConnections <- tcpConnection
				} else {
					// 将无法修复的坏连接给回自己的池子
					// 延迟1秒后再次尝试
					m.MQConnections[channelKey] <- tcpConnection
					time.Sleep(time.Second)
				}
			} else {
				m.AllConnections <- tcpConnection
			}
		}
	}()
}

// 获取一个连接
func (m *MQConnectionPool) GetAConnection() (*MQConnection, error) {
	if !m.Status {
		return nil, errors.New("无法获取有效连接 E200100")
	}
	select {
	case tcpConnection := <-m.AllConnections:
		return tcpConnection, nil
	default:
		return nil, errors.New("无法获取有效连接 E200101")
	}
}

// 获取 MQ 服务列表
func (m *MQConnectionPool) GetMQServerAddresses() error {
	firstKVConn, err := net.DialTimeout("tcp", m.FirstKVAddr, time.Second*5)
	if err != nil {
		return err
	}
	res, err := firstKV.Send(
		firstKVConn,
		firstKV.ReceiveMessage{
			Action:  "get",
			MainKey: "firstMQServers",
		},
		false,
	)
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(res.Data), &m.Addresses)
	if err != nil {
		return err
	}
	// 验证 MQ 服务
	for k, address := range m.Addresses {
		addr := address.Data.(string)
		connIn, err := net.DialTimeout("tcp", addr, time.Second*3)
		// 验证失败将其移除
		if err != nil {
			delete(m.Addresses, k)
			_, err = firstKV.Send(
				firstKVConn,
				firstKV.ReceiveMessage{
					MainKey: "firstMQServers",
					Action:  "removeItem",
					ItemKey: addr,
				},
				true,
			)
			if err == nil {
				log.Println("MQServer ", address, " 验证失败, 已将其移除.")
			}
		} else {
			connIn.Close()
		}
	}
	return nil
}
