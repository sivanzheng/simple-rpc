package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"rpc/codec"
	"strings"
	"sync"
	"time"
)

const MAGIC_NUMBER = 0x3bef5c

type Option struct {
	MagicNumber    int           // 标记这是一个 simple-rpc 请求
	CodecType      codec.Type    // 客户端可以选择不同的 Codec 来编码 body
	ConnectTimeout time.Duration // 0 表示不设置超时
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MAGIC_NUMBER,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server 代表一个 RPC 服务器
type Server struct {
	serviceMap sync.Map
}

// NewServer 创建 RPC 服务器
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是 *Server 的默认实例
var DefaultServer = NewServer()

// Accept 在侦听连接并为每个传入连接提供请求
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

// Accept 在侦听连接并为每个传入连接提供请求
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
	server.serveCodec(codecFunc(conn), &option)
}

// invalidRequest 是发生错误时响应 argv 的占位符
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
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
		go server.handleRequest(cc, req, sending, waitGroup, opt.HandleTimeout)
	}
	waitGroup.Wait()
	_ = cc.Close()
}

// request 存储了调用的全部信息
type request struct {
	h          *codec.Header
	argValue   reflect.Value
	replyValue reflect.Value
	mType      *methodType
	svc        *service
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
	req.svc, req.mType, err = server.findService(header.ServiceMethod)
	if err != nil {
		return req, err
	}

	req.argValue = req.mType.newArgValue()
	req.replyValue = req.mType.newReplyValue()

	// 如果参数不是指针，需要创建一个指针, ReadBody 需要一个指针作为参数
	argValueInterface := req.argValue.Interface()
	if req.argValue.Type().Kind() != reflect.Ptr {
		argValueInterface = req.argValue.Addr().Interface()
	}
	if err = cc.ReadBody(argValueInterface); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
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

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mType, req.argValue, req.replyValue)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyValue.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Register 在服务器中发布一组方法，这些方法的接收器是 serviceObject
func (server *Server) Register(serviceObject interface{}) error {
	s := newService(serviceObject)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register 在 DefaultServer 中发布接收器的方法
func Register(serviceObject interface{}) error {
	return DefaultServer.Register(serviceObject)
}

const (
	connected        = "200 Connected to RPC"
	defaultRPCPath   = "/_rpc_"
	defaultDebugPath = "/debug/rpc"
)

// ServerHTTP 接收 CONNECT 类型的 HTTP 请求，并通过底层 TCP 连接进行 RPC 调用的处理
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP 在 rpcPath 上注册 RPC 消息的 HTTP 处理程序
// 通常在 go 中仍然需要调用 http.Serve()
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP 是默认服务器注册 HTTP 处理程序的便捷方法
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

// findService 返回服务和方法的类型
func (server *Server) findService(serviceMethod string) (svc *service, mType *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mType = svc.method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}
