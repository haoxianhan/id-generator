package generator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// 添加通用的验证方法
func validateIDs(t *testing.T, ids []int64, expectedCount int) {
	t.Helper()

	t.Run("验证ID数量", func(t *testing.T) {
		if len(ids) != expectedCount {
			t.Errorf("期望生成 %d 个ID，实际生成 %d 个", expectedCount, len(ids))
		}
	})

	t.Run("验证ID唯一性和递增性", func(t *testing.T) {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for i := 1; i < len(ids); i++ {
			if ids[i] == ids[i-1] {
				t.Errorf("发现重复ID: %d", ids[i])
			}
			if ids[i] <= ids[i-1] {
				t.Errorf("ID序列不是严格递增的: ids[%d]=%d, ids[%d]=%d",
					i-1, ids[i-1], i, ids[i])
			}
		}
	})
}

// 添加性能统计方法
func logPerformanceStats(t *testing.T, ids []int64, duration time.Duration) {
	t.Helper()

	idsPerSecond := float64(len(ids)) / duration.Seconds()
	t.Logf("性能统计:\n"+
		"总耗时: %v\n"+
		"总ID数: %d\n"+
		"每秒生成ID数: %.2f\n"+
		"最小ID: %d\n"+
		"最大ID: %d",
		duration, len(ids), idsPerSecond, ids[0], ids[len(ids)-1])
}

func TestSegmentIDGeneratorConcurrent(t *testing.T) {
	// 测试配置
	mongoURI := "mongodb://test:123456@192.168.110.128:27017/?authMechanism=SCRAM-SHA-1"
	bizTag := fmt.Sprintf("test_concurrent_%d", time.Now().Unix())

	// 初始化生成器
	gen, err := NewSegmentIDGenerator(mongoURI, bizTag)
	if err != nil {
		t.Fatalf("创建ID生成器失败: %v", err)
	}
	defer gen.Close()

	// 测试参数
	goroutineCount := 10   // 并发协程数
	idsPerGoroutine := 100 // 每个协程生成的ID数量
	totalIDs := goroutineCount * idsPerGoroutine

	// 用于收集生成的ID
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = make([]int64, 0, totalIDs)
	)

	// 启动并发测试
	startTime := time.Now()
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localIDs := make([]int64, 0, idsPerGoroutine)

			for j := 0; j < idsPerGoroutine; j++ {
				id, err := gen.NextID()
				if err != nil {
					t.Errorf("生成ID失败: %v", err)
					return
				}
				localIDs = append(localIDs, id)
			}

			mu.Lock()
			ids = append(ids, localIDs...)
			mu.Unlock()
		}()
	}

	// 等待所有协程完成
	wg.Wait()
	duration := time.Since(startTime)

	// 使用通用验证方法
	validateIDs(t, ids, goroutineCount*idsPerGoroutine)

	// 验证MongoDB中的记录
	t.Run("验证MongoDB记录", func(t *testing.T) {
		ctx := context.Background()
		var result struct {
			MaxID int64 `bson:"maxId"`
		}

		err := gen.mongoClient.Database("test").Collection("segments").
			FindOne(ctx, bson.M{"_id": bizTag}).Decode(&result)

		if err != nil {
			if err == mongo.ErrNoDocuments {
				t.Error("MongoDB中未找到记录")
			} else {
				t.Errorf("查询MongoDB失败: %v", err)
			}
			return
		}

		maxGeneratedID := ids[len(ids)-1]
		if result.MaxID < maxGeneratedID {
			t.Errorf("MongoDB中记录的maxId(%d)小于最大生成的ID(%d)",
				result.MaxID, maxGeneratedID)
		}
	})

	// 使用通用性能统计方法
	logPerformanceStats(t, ids, duration)
}

func TestSegmentIDGeneratorWithHTTP(t *testing.T) {

	// 测试参数
	goroutineCount := 20
	requestsPerGoroutine := 5000
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	ids := make(chan int64, goroutineCount*requestsPerGoroutine)
	startTime := time.Now()

	// 并发发送请求
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				resp, err := client.Get("http://localhost:8080/id")
				if err != nil {
					t.Errorf("Request failed: %v", err)
					continue
				}

				// 读取响应体
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Errorf("Failed to read response body: %v", err)
					continue
				}

				// 直接将响应转换为int64
				id, err := strconv.ParseInt(string(body), 10, 64)
				if err != nil {
					t.Errorf("Failed to parse ID: %v", err)
					continue
				}
				ids <- id
			}
		}()
	}

	// 等待所有请求完成
	go func() {
		wg.Wait()
		close(ids)
	}()

	// 收集和验证结果
	var receivedIDs []int64
	for id := range ids {
		receivedIDs = append(receivedIDs, id)
	}

	duration := time.Since(startTime)

	// 使用通用验证方法
	validateIDs(t, receivedIDs, goroutineCount*requestsPerGoroutine)

	// 使用通用性能统计方法
	logPerformanceStats(t, receivedIDs, duration)
}
