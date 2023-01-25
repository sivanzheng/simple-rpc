package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"rpc/codec"
	"sync"
)

const MAGIC_NUMBER = 0x3bef5c

type Option struct {
	MagicNumber int        // 标记这是一个 simple-rpc 请求
	CodecType   codec.Type // 客户端可以选择不同的 Codec 来编码 body
}

var DefaultOption = &Option{
	MagicNumber: MAGIC_NUMBER,
	CodecType:   codec.GobType,
}

// Server 代表一个 RPC 服务器
type Server struct{}

// NewServer 创建 RPC 服务器
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是 *Server 的默认实例
var DefaultServer = NewServer()

// Accept 在侦听连接并为每个传入连接提供请求。
func (server *Server) Accept(listener net.Listener) {
	// net.Listener 作为参数
	// for 循环等待 socket 连接建立
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[RPC Server] accept error: ", err)
			return
		}
		// 开启子协程处理，处理过程交给 ServerConn 方法
		go server.ServeConn(conn)
	}
}

// Accept 在侦听连接并为每个传入连接提供请求。
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()

	var option Option

	if err := json.NewDecoder(conn).Decode(&option); err != nil {
		log.Println("[RPC Server] options error ", err)
		return
	}

	if option.MagicNumber != MAGIC_NUMBER {
		log.Printf("[RPC Server] invalid magic number %x", option.MagicNumber)
		return
	}

	codecFunc := codec.NewCodecFuncMap[option.CodecType]
	if codecFunc == nil {
		log.Printf("[RPC Server] invalid codec type %s", option.CodecType)
		return
	}
	server.serveCodec(codecFunc(conn))
}

// invalidRequest 是发生错误时响应 argv 的占位符
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec) {
	// 确保发送了完整的响应
	sending := new(sync.Mutex)

	// 等到所有请求都被处理
	waitGroup := new(sync.WaitGroup)

	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 不能恢复，直接关闭连接
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		waitGroup.Add(1)
		go server.handleRequest(cc, req, sending, waitGroup)
	}
	waitGroup.Wait()
	_ = cc.Close()
}

// request 存储了调用的全部信息
type request struct {
	h     *codec.Header
	argv  reflect.Value
	reply reflect.Value
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var header codec.Header
	if err := cc.ReadHeader(&header); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("[RPC Server] read header error: ", err)
		}
		return nil, err
	}
	return &header, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: header}
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("[RPC Server] read argv err:", err)
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("[RPC Server] write response error: ", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.reply = reflect.ValueOf(fmt.Sprintf("rpc res %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.reply.Interface(), sending)
}
