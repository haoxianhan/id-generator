package generator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Segment struct {
	Min     int64
	Max     int64
	Current int64
}

type SegmentIDGenerator struct {
	currentSegment atomic.Pointer[Segment] // 当前使用的号段
	nextSegment    atomic.Pointer[Segment] // 下一个号段
	bizTag         string
	step           int32
	mongoClient    *mongo.Client
	loadingFlag    atomic.Bool
}

func NewSegmentIDGenerator(mongoURI, bizTag string) (*SegmentIDGenerator, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("connect mongodb failed: %v", err)
	}

	generator := &SegmentIDGenerator{
		bizTag:      bizTag,
		mongoClient: client,
		step:        1000,
	}

	// 初始化时加载第一个号段作为当前号段
	if err := generator.loadNextSegment(); err != nil {
		return nil, err
	}

	// 将加载的号段设置为当前号段
	generator.currentSegment.Store(generator.nextSegment.Load())
	generator.nextSegment.Store(nil)

	return generator, nil
}

func (g *SegmentIDGenerator) NextID() (int64, error) {
	current := g.currentSegment.Load()
	if current == nil {
		return 0, fmt.Errorf("no available segment")
	}

	// 获取下一个ID
	id := atomic.AddInt64(&current.Current, 1)
	if id > current.Max {
		// 如果超出当前号段范围，尝试切换到下一个号段
		if err := g.switchSegment(); err != nil {
			return 0, err
		}
		return g.NextID()
	}

	// 检查是否需要预加载下一个号段
	if g.shouldLoadNext(current) {
		go g.loadNextSegment()
	}

	return id, nil
}

func (g *SegmentIDGenerator) shouldLoadNext(segment *Segment) bool {
	if segment == nil {
		return true
	}

	// 当前号段已使用量超过80%且没有下一个号段时加载
	used := segment.Current - segment.Min + 1
	total := segment.Max - segment.Min + 1
	return float64(used)/float64(total) >= 0.8 &&
		g.nextSegment.Load() == nil &&
		!g.loadingFlag.Load()
}

func (g *SegmentIDGenerator) switchSegment() error {
	// 如果没有下一个号段，等待加载
	for g.nextSegment.Load() == nil {
		if err := g.loadNextSegment(); err != nil {
			return err
		}
	}

	// 切换号段
	g.currentSegment.Store(g.nextSegment.Load())
	g.nextSegment.Store(nil)
	return nil
}

func (g *SegmentIDGenerator) loadNextSegment() error {
	if !g.loadingFlag.CompareAndSwap(false, true) {
		return nil
	}
	defer g.loadingFlag.Store(false)

	filter := bson.M{"_id": g.bizTag}
	update := bson.M{
		"$inc": bson.M{
			"maxId": g.step,
		},
		"$setOnInsert": bson.M{
			"initTime": time.Now(),
		},
	}

	var result struct {
		MaxID int64 `bson:"maxId"`
	}

	coll := g.mongoClient.Database("test").Collection("segments")

	err := coll.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		options.FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetUpsert(true),
	).Decode(&result)

	if err != nil {
		return fmt.Errorf("load segment failed: %v", err)
	}

	newSegment := &Segment{
		Min:     result.MaxID - int64(g.step) + 1,
		Max:     result.MaxID,
		Current: result.MaxID - int64(g.step),
	}

	g.nextSegment.Store(newSegment)
	return nil
}

func (g *SegmentIDGenerator) Close() error {
	if g.mongoClient != nil {
		return g.mongoClient.Disconnect(context.Background())
	}
	return nil
}
