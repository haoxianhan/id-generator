#!/bin/bash

# 测试配置
HOST="172.28.252.61"
PORT="8080"
CONCURRENT=12   # 并发数
REQUESTS=2000    # 每个并发请求数

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# 测试单个ID生成
echo -e "${GREEN}测试单个ID生成:${NC}"
curl -s "http://$HOST:$PORT/id"
echo -e "\n"

# 测试批量ID生成
echo -e "${GREEN}测试批量生成10个ID:${NC}"
curl -s "http://$HOST:$PORT/batch?count=10"
echo -e "\n"

# 并发测试
echo -e "${GREEN}开始并发测试...${NC}"
echo "并发数: $CONCURRENT, 每个并发请求数: $REQUESTS"

# 创建临时文件存储结果
TEMP_FILE=$(mktemp)

# 并发请求函数
function concurrent_test() {
    for ((i=1; i<=$REQUESTS; i++)); do
        # 添加重试逻辑
        for ((retry=0; retry<3; retry++)); do
            ID=$(curl -s "http://$HOST:$PORT/id")
            if [[ ! -z "$ID" ]] && [[ "$ID" =~ ^[0-9]+$ ]]; then
                echo "$ID" >> "$TEMP_FILE"
                break
            fi
            ## sleep 0.1  # 失败后短暂等待
        done
    done
}

# 启动并发测试
for ((i=1; i<=$CONCURRENT; i++)); do
    concurrent_test &
done

# 等待所有后台任务完成
wait

# 移除空行（如果有的话）
sed -i '/^$/d' "$TEMP_FILE"

# 检查结果
TOTAL_IDS=$(cat "$TEMP_FILE" | wc -l)
UNIQUE_IDS=$(sort -u "$TEMP_FILE" | wc -l)

echo -e "${GREEN}测试完成${NC}"
echo "总请求数: $TOTAL_IDS"
echo "唯一ID数: $UNIQUE_IDS"

if [ "$TOTAL_IDS" -eq "$UNIQUE_IDS" ]; then
    echo -e "${GREEN}所有ID都是唯一的${NC}"
else
    echo -e "${RED}警告: 存在重复ID${NC}"
    echo "重复ID数: $(($TOTAL_IDS - $UNIQUE_IDS))"
    # 输出重复的ID
    echo -e "${RED}重复的ID:${NC}"
    sort "$TEMP_FILE" | uniq -d
fi

# 保存临时文件路径
rm "$TEMP_FILE"
