package firstMQClient

import (
	"encoding/json"
	"errors"

	"github.com/cnlesscode/gotool"
)

// 记录错误消息到缓存通道
func RecordErrorMessage(message []byte, k string) {
	select {
	case MQPoolMap[k].ErrorMessage <- message:
		return
	default:
		return
	}
}

// 发送消息
func (st *MQConnectionPool) Send(message Message) (ResponseMessage, error) {
	response := ResponseMessage{}
	messageByte, err := json.Marshal(message)
	if err != nil {
		return response, err
	}
	// 获取一个可用连接
	mqClient, err := st.GetAConnection()
	if err != nil {
		RecordErrorMessage(messageByte, st.Key)
		return response, err
	}

	res, err := mqClient.SendBytes(messageByte)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(res, &response)
	if err != nil {
		return response, err
	}
	if response.ErrCode != 0 {
		return response, errors.New(response.Data)
	}

	return response, nil
}

// send []byte
func (st *TCPConnection) SendBytes(message []byte) ([]byte, error) {
	defer func() {
		// 如果是新建的连接关闭连接
		// 如果是来自连接池的连接填充回连接池
		if st.MapKey == "" {
			st.Conn.Close()
		} else {
			MQPoolMap[st.MapKey].Channels[st.Addr] <- st
		}
	}()

	if st.Conn == nil {
		RecordErrorMessage(message, st.MapKey)
		st.Status = false
		return nil, errors.New("TCP 服务错误")
	}

	// ------ 发送消息 ------
	err := gotool.WriteTCPResponse(st.Conn, message)
	if err != nil {
		RecordErrorMessage(message, st.MapKey)
		st.Status = false
		return nil, err
	}

	// ------ 接收消息 ------
	buf, err := gotool.ReadTCPResponse(st.Conn)
	if err != nil {
		RecordErrorMessage(message, st.MapKey)
		st.Status = false
		st.Conn.Close()
		return nil, err
	}

	return buf, nil
}
