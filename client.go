package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"rpc/codec"
	"sync"
)

type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec // 消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应
	option   *Option
	sending  sync.Mutex   // 互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header   codec.Header // 每个请求的消息头只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个
	mutex    sync.Mutex
	seq      uint64           // 发送的请求编号，每个请求拥有唯一编号
	pending  map[uint64]*Call // 存储未处理完的请求，键是编号，值是 Call 实例
	closing  bool             // 用户主动关闭的，即调用 Close 方法
	shutdown bool             //  Client 有错误发生处于不可用的状态
}

var _ io.Closer = &Client{}

var ErrShutdown = errors.New("connection is shut down")

func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Lock()

	if client.closing {
		return ErrShutdown
	}

	client.closing = true
	return client.cc.Close()
}

func (client *Client) IsAvailable() bool {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall 根据 seq，从 client.pending 中移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls 服务端或客户端发生错误时调用
// 将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mutex.Lock()
	defer client.mutex.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		var header codec.Header
		if err = client.cc.ReadHeader(&header); err != nil {
			break
		}
		call := client.removeCall(header.Seq)

		// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
		// 通常表示 Write 部分失败, 调用已经被移除
		if call == nil {
			err = client.cc.ReadBody(nil)
			break
		}

		// call 存在，但服务端处理出错，即 header.Error 不为空
		if header.Error != "" {
			call.Error = fmt.Errorf(header.Error)
			err = client.cc.ReadBody(nil)
			call.done()
			break
		}

		// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值
		err = client.cc.ReadBody(call.Reply)
		if err != nil {
			call.Error = errors.New("reading body " + err.Error())
		}
		call.done()
	}
	// 发生错误，因此终止所有挂起的 call
	client.terminateCalls(err)
}

// NewClient 创建 Client，首先需要完成一开始的协议交换，即发送 Option 信息给服务端
func NewClient(conn net.Conn, option *Option) (*Client, error) {
	codecFunc := codec.NewCodecFuncMap[option.CodecType]
	if codecFunc == nil {
		err := fmt.Errorf("invalid codec type %s", option.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(option); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(codecFunc(conn), option), nil
}

// newClientCodec 协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应
func newClientCodec(cc codec.Codec, option *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		option:  option,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

// parseOptions 为了简化用户调用，通过将 Option 实现为可选参数
func parseOptions(options ...*Option) (*Option, error) {
	// 如果 option 是 nil 或者传递 nil 作为参数
	if len(options) == 0 || options[0] == nil {
		return DefaultOption, nil
	}
	if len(options) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	option := options[0]
	option.MagicNumber = DefaultOption.MagicNumber
	if option.CodecType == "" {
		option.CodecType = DefaultOption.CodecType
	}
	return option, nil
}

// Dial 连接到指定网络地址的 RPC 服务器
func Dial(network string, address string, options ...*Option) (client *Client, err error) {
	option, err := parseOptions(options...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, option)
}

func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册 call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步调用该函数，它返回表示调用的 Call 结构
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is no buffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 是对 Go 的封装，阻塞 call.Done，等待响应返回，是一个同步接口
func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
