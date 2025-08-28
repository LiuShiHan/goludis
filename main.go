package main

import (
	"fmt"
	"goludis/cache"
	"time"
)

func main() {
	//db, _ := buntdb.Open("data.db") // 或 ":memory:"
	//defer db.Close()
	//
	//_ = db.Update(func(tx *buntdb.Tx) error {
	//	tx.Set("name", "alice", nil)
	//	tx.Set("cnt", "1", &buntdb.SetOptions{Expires: true, TTL: time.Minute})
	//	tx.Delete("name")
	//	return nil
	//})
	//
	//_ = db.View(func(tx *buntdb.Tx) error {
	//	v, _ := tx.Get("name")
	//	fmt.Println(v) // alice
	//	return nil
	//})

	mydb, err := cache.NewCache[string, string](4)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = mydb.Set("aaa", "bbb", &cache.SetOptions{Expires: true, TTL: time.Second * 2})
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(time.Second * 5)
	val, err := mydb.Get("aaa")

	fmt.Println(val, err)

}
