package firstMQClient

import (
	"encoding/json"
	"net"
	"time"

	"github.com/cnlesscode/firstKV"
	"github.com/cnlesscode/gotool"
)

func (m *MQConnectionPool) FirstKVSendMessage(msg firstKV.ReceiveMessage) (ResponseMessage, error) {
	response := ResponseMessage{}
	// 整理服务地址
	conn, err := net.DialTimeout("tcp", m.FirstKVAddr, time.Second*5)
	if err != nil {
		return response, err
	}
	defer conn.Close()

	message, err := json.Marshal(msg)
	if err != nil {
		return response, err
	}
	gotool.WriteTCPResponse(conn, message)

	// 02. 读取消息
	buf, err := gotool.ReadTCPResponse(conn)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(buf, &response)
	if err != nil {
		return response, err
	}
	return response, nil
}
