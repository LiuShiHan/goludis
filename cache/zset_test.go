package cache

import (
	"fmt"
	"log"
	"testing"
)

func Test_Zset(t *testing.T) {
	c, err := NewCache[string, string](4) // 4 个分片
	if err != nil {
		log.Fatal(err)
	}
	c.ZAdd("rank", 100, "tom")
	c.ZAdd("rank", 90, "bob")
	c.ZAdd("rank", 110, "alice")
	fmt.Println(c.ZRange("rank", 0, -1))   // [bob tom alice]
	fmt.Println(c.ZRevRange("rank", 1, 1)) // [alice tom]
}
