package rpc

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"reflect"
	"rpc/codec"
	"strings"
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

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	err := req.svc.call(req.mType, req.argValue, req.replyValue)
	if err != nil {
		req.h.Error = err.Error()
		server.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}
	server.sendResponse(cc, req.h, req.replyValue.Interface(), sending)
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
