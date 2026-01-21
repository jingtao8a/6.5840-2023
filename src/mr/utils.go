package mr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"
)

func GenerateSimpleUniqueID() string {
	// 1. 获取毫秒级时间戳（保证时间维度唯一）
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	// 2. 获取进程ID（区分同一台机器的不同进程）
	pid := strconv.Itoa(os.Getpid())
	// 3. 生成随机数（避免同一进程同一毫秒生成重复ID）
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(fmt.Sprintf("生成随机数失败: %v", err))
	}
	randomStr := hex.EncodeToString(randomBytes)

	// 拼接所有部分，生成最终ID
	return fmt.Sprintf("%s-%s-%s", timestamp, pid, randomStr)
}
