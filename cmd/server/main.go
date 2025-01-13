package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/haoxianhan/id-generator/internal/generator"
)

func main() {
	// 命令行参数
	mongoURI := flag.String("mongo", "mongodb://test:123456@192.168.110.128:27017/?authMechanism=SCRAM-SHA-1", "MongoDB URI")
	bizTag := flag.String("biz", "default", "Business tag")
	host := flag.String("host", "0.0.0.0", "HTTP server host")
	port := flag.Int("port", 8080, "HTTP server port")
	flag.Parse()

	// 创建ID生成器
	gen, err := generator.NewSegmentIDGenerator(*mongoURI, *bizTag)
	if err != nil {
		log.Fatalf("Failed to create ID generator: %v", err)
	}
	defer gen.Close()

	// HTTP处理函数
	http.HandleFunc("/id", func(w http.ResponseWriter, r *http.Request) {
		id, err := gen.NextID()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Println(id)
		fmt.Fprintf(w, "%d", id)
	})

	// 批量获取ID
	http.HandleFunc("/batch", func(w http.ResponseWriter, r *http.Request) {
		countStr := r.URL.Query().Get("count")
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 || count > 1000 {
			count = 1
		}

		ids := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			id, err := gen.NextID()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			ids = append(ids, id)
		}

		// 简单的拼接输出，用逗号分隔
		for i, id := range ids {
			if i > 0 {
				fmt.Fprintf(w, ",")
			}
			fmt.Fprintf(w, "%d", id)
		}
	})

	// 启动服务
	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
