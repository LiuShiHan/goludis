package cache

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func Test_List(t *testing.T) {
	c, err := NewCache[string, string](2) // 4 个分片
	if err != nil {
		log.Fatal(err)
	}
	const key = "jobs"

	// 2. 启动 3 个消费者
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		id := i
		wg.Add(1)

		go func() {
			defer wg.Done()

			v, err := c.BRPop(key, 3*time.Second) // 3 秒超时
			if err != nil {
				fmt.Printf("worker-%d timeout, exit\n", id)
				return
			}
			fmt.Printf("worker-%d got job: %s\n", id, v)

		}()
	}

	// 3. 主 goroutine 生产 6 条消息
	time.Sleep(200 * time.Millisecond) // 让工作 goroutine 先阻塞
	for i := 1; i <= 6; i++ {

		msg := fmt.Sprintf("task-%02d", i)
		c.LPush(key, msg)
		fmt.Printf("producer pushed: %s\n", msg)
		time.Sleep(3 * time.Second)
	}

	// 4. 等消费者全部退出
	wg.Wait()
	fmt.Println("all workers exit, main done")
}
