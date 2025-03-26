package firstMQClient

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/cnlesscode/serverFinder"
)

// 已有连接池 map
// 利用 map 键实现单利
var MQPoolMap map[string]*MQConnectionPool = make(map[string]*MQConnectionPool)

// 建立连接池 :
// ServerFinderAddr  ServerFinder 服务地址,
// capacity 连接池容量,
// purpose 用途，由于单例区分
func New(ServerFinderAddr string, capacity int, purpose, listenAddr string) (*MQConnectionPool, error) {
	mapKey := ServerFinderAddr + purpose
	_, ok := MQPoolMap[mapKey]
	// 已有连接池直接返回
	if ok {
		return MQPoolMap[mapKey], nil
	}
	// 新建连接池
	MQPoolMap[mapKey] = &MQConnectionPool{
		Key:            mapKey,
		ServerFindAddr: ServerFinderAddr,
		Addresses:      nil,
		AddressesLen:   0,
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

	// 创建监听
	if listenAddr != "" {
		serverFinder.Listen(listenAddr, "firstMQServers", func(message map[string]any) {
			fmt.Printf("message: %v\n", message)
			if len(message) < 1 {
				return
			}
			MQPoolMap[mapKey].Init(message)
		})
	}

	// 初始化连接池
	err := MQPoolMap[mapKey].Init(nil)
	if err != nil {
		return MQPoolMap[mapKey], err
	}

	// 监听错误消息并自动发送
	go func(mapKeyIn string) {
		time.Sleep(time.Second * 3)
		for {
			errorMessagesCount := len(MQPoolMap[mapKeyIn].ErrorMessage)
			if errorMessagesCount < 1 {
				time.Sleep(time.Second)
				continue
			}
			// 启动对应连接数数量的协程重发错误消息
			var wg sync.WaitGroup
			for i := 0; i < MQPoolMap[mapKeyIn].Capacity; i++ {
				wg.Add(1)
				go func(mapKeyIn string) {
					defer wg.Done()
					select {
					case message := <-MQPoolMap[mapKeyIn].ErrorMessage:
						conn, err := MQPoolMap[mapKeyIn].GetAConnection()
						// 如果有错再放回错误连消息chi
						if err != nil {
							select {
							case MQPoolMap[mapKeyIn].ErrorMessage <- message:
								return
							default:
								return
							}
						} else {
							conn.SendBytes(message)
						}
					default:
						return
					}
				}(mapKeyIn)
			}
			wg.Wait()
			continue
		}
	}(mapKey)

	return MQPoolMap[mapKey], err
}

func (m *MQConnectionPool) Init(addrs map[string]any) error {
	if addrs == nil {
		// 整理服务地址
		err := m.GetMQServerAddresses()
		if err != nil {
			m.Status = false
			return errors.New("无可用服务 E10001")
		}
	} else {
		m.Addresses = addrs
		fmt.Printf("\"----\": %v\n", "----")
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
				fmt.Printf("\"已有节点\": %v\n", "已有节点")
				m.ConnDifferenceNumber[channelKey] = capacityForEveryServer - m.ConnNumber[channelKey]
				fmt.Printf("m.ConnDifferenceNumber[channelKey]: %v\n", m.ConnDifferenceNumber[channelKey])
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
	fmt.Printf("\"InitNewNode\": %v\n", "InitNewNode")
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
	serverFinderConn, err := net.DialTimeout("tcp", m.ServerFindAddr, time.Second*5)
	if err != nil {
		return err
	}
	res, err := serverFinder.Send(
		serverFinderConn,
		serverFinder.ReceiveMessage{
			Action:  "get",
			MainKey: "firstMQServers",
		},
		false,
	)
	if err != nil {
		return err
	}
	if res.ErrorCode != 0 {
		return errors.New(res.Data.(string))
	}
	var ok bool = false
	m.Addresses, ok = res.Data.(map[string]any)
	if !ok {
		return errors.New("服务器列表获取失败，E200102")
	}
	// 验证 MQ 服务
	for k := range m.Addresses {
		connIn, err := net.DialTimeout("tcp", k, time.Second*3)
		// 验证失败将其移除
		if err != nil {
			delete(m.Addresses, k)
			_, err = serverFinder.Send(
				serverFinderConn,
				serverFinder.ReceiveMessage{
					MainKey: "firstMQServers",
					Action:  "removeItem",
					ItemKey: k,
				},
				true,
			)
			if err == nil {
				log.Println("MQServer ", k, " 验证失败, 已将其移除.")
			}
		} else {
			connIn.Close()
		}
	}
	return nil
}
