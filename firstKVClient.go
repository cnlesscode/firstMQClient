package firstMQClient

import (
	"encoding/json"
	"net"
	"time"
)

func (m *MQConnectionPool) FirstKVSendMessage(msg FirstKVMessage) (ResponseMessage, error) {
	response := ResponseMessage{}
	// 整理服务地址
	conn, err := net.DialTimeout("tcp", m.FirstKVAddr, time.Second*5)
	if err != nil {
		return response, err
	}
	defer conn.Close()
	messageST := FirstKVMessage{
		Action: "get mqServers",
		Key:    "firstMQServers",
		Data:   FirstMQAddr{},
	}
	message, _ := json.Marshal(messageST)
	conn.Write(message)
	// 读取消息
	buf := make([]byte, 102400)
	n, err := conn.Read(buf)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(buf[0:n], &response)
	if err != nil {
		return response, err
	}
	return response, nil
}
