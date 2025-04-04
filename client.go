package firstMQClient

import (
	"net"
	"time"
)

// 初始化一个TCP连接
// 非连接池模式
func NewAClient(addr string) (*MQConnection, error) {
	tcpConnection := &MQConnection{
		Addr: addr,
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second*3)
	if err != nil {
		tcpConnection.Status = false
		return tcpConnection, err
	}
	tcpConnection.Conn = conn
	tcpConnection.Status = true
	return tcpConnection, nil
}

func (m *MQConnectionPool) NewAClientForPool(addr string) (*MQConnection, error) {
	tcpConnection, err := NewAClient(addr)
	tcpConnection.MapKey = m.Key
	return tcpConnection, err
}

// 关闭连接
func (st *MQConnection) Close() {
	st.Status = false
	st.Conn.Close()
}
