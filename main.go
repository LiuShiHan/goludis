package main

import (
	"fmt"
	"goludis/cache"
	"time"
)

func main() {

	mydb, err := cache.NewCache[string, string](4)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = mydb.Set("aaa", "bbb", &cache.SetOptions{Expires: true, TTL: time.Second * 10})

	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second * 5)
	val, err := mydb.Get("aaa")

	fmt.Println(val, err)

}
