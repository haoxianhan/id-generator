package generator

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Segment 表示一个号段
type Segment struct {
	Min      int64 // 号段最小值
	Max      int64 // 号段最大值
	Current  int64 // 当前值
	IsActive bool  // 是否是活跃号段
}

// SegmentIDGenerator 号段生成器
type SegmentIDGenerator struct {
	segments    [2]*Segment // 双buffer，保存两个号段
	bizTag      string      // 业务标签
	step        int32       // 固定步长
	mongoClient *mongo.Client
	loadingFlag atomic.Bool // 是否正在加载新号段
}

// NewSegmentIDGenerator 创建新的号段生成器
func NewSegmentIDGenerator(mongoURI, bizTag string) (*SegmentIDGenerator, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("connect mongodb failed: %v", err)
	}

	generator := &SegmentIDGenerator{
		bizTag:      bizTag,
		mongoClient: client,
		step:        1000, // 固定步长
	}

	if err := generator.loadNextSegment(); err != nil {
		return nil, err
	}

	return generator, nil
}

// NextID 获取下一个ID
func (g *SegmentIDGenerator) NextID() (int64, error) {
	segment := g.getCurrentSegment()
	if segment == nil {
		return 0, fmt.Errorf("no available segment")
	}

	// 如果当前号段使用量超过80%，异步加载下一个号段
	if g.shouldLoadNext(segment) {
		go g.loadNextSegment()
	}

	current := atomic.AddInt64(&segment.Current, 1)
	if current > segment.Max {
		g.switchToNextSegment()
		return g.NextID()
	}

	return current, nil
}

// loadNextSegment 加载下一个号段
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
	}

	var result struct {
		MaxID int64 `bson:"maxId"`
	}

	err := g.mongoClient.Database("test").Collection("segments").
		FindOneAndUpdate(
			context.Background(),
			filter,
			update,
			options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true),
		).Decode(&result)

	if err != nil {
		return fmt.Errorf("load segment failed: %v", err)
	}

	newSegment := &Segment{
		Min:      result.MaxID - int64(g.step) + 1,
		Max:      result.MaxID,
		Current:  result.MaxID - int64(g.step),
		IsActive: false,
	}

	g.setNextSegment(newSegment)
	return nil
}

// getCurrentSegment 获取当前活跃号段
func (g *SegmentIDGenerator) getCurrentSegment() *Segment {
	for _, segment := range g.segments {
		if segment != nil && segment.IsActive {
			return segment
		}
	}
	return nil
}

// setNextSegment 设置下一个号段
func (g *SegmentIDGenerator) setNextSegment(newSegment *Segment) {
	for i, segment := range g.segments {
		if segment == nil || !segment.IsActive {
			g.segments[i] = newSegment
			return
		}
	}
}

// switchToNextSegment 切换到下一个号段
func (g *SegmentIDGenerator) switchToNextSegment() {
	for i, segment := range g.segments {
		if segment != nil {
			if segment.IsActive {
				segment.IsActive = false
			} else {
				g.segments[i].IsActive = true
			}
		}
	}
}

// shouldLoadNext 判断是否应该加载下一个号段
func (g *SegmentIDGenerator) shouldLoadNext(segment *Segment) bool {
	if segment == nil {
		return true
	}
	used := segment.Current - segment.Min + 1
	total := segment.Max - segment.Min + 1
	return float64(used)/float64(total) >= 0.8 && !g.loadingFlag.Load()
}

// Close 关闭生成器
func (g *SegmentIDGenerator) Close() error {
	if g.mongoClient != nil {
		return g.mongoClient.Disconnect(context.Background())
	}
	return nil
}
