package main

import (
	"fmt"
	"github.com/tidwall/buntdb"
	"time"
)

func main() {
	db, _ := buntdb.Open("data.db") // 或 ":memory:"
	defer db.Close()

	_ = db.Update(func(tx *buntdb.Tx) error {
		tx.Set("name", "alice", nil)
		tx.Set("cnt", "1", &buntdb.SetOptions{Expires: true, TTL: time.Minute})
		return nil
	})

	_ = db.View(func(tx *buntdb.Tx) error {
		v, _ := tx.Get("name")
		fmt.Println(v) // alice
		return nil
	})
}
