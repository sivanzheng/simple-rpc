package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"rpc"
	"rpc/codec"
	"time"
)

func startServer(addr chan string) {
	// pick a free port
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	rpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)
	fmt.Println(<-addr)

	// rpc 客户端
	conn, _ := net.Dial("tcp", <-addr)
	defer func() {
		_ = conn.Close()
	}()

	time.Sleep(time.Second)
	// 发送 options
	_ = json.NewEncoder(conn).Encode(rpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	// send request & receive response
	for i := 0; i < 5; i++ {
		header := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           fmt.Sprintf("%v", i),
		}
		_ = cc.Write(header, fmt.Sprintf("rpc req %s", header.Seq))
		_ = cc.ReadHeader(header)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
