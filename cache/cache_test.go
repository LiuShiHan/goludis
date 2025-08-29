package cache

import (
	"fmt"
	"testing"
)

func Test_CacheNew(t *testing.T) {
	db, err := NewCache[string, string](3)
	if err != nil {
		t.Fatal(err)
	}
	db.Set("aaa", "bbb", nil)
	val, err := db.Get("aaa")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
}

func Test_CacheDelete(t *testing.T) {
	db, err := NewCache[string, string](3)
	if err != nil {
		t.Fatal(err)
	}
	db.Set("aaa", "bbb", nil)
	val, err := db.Get("aaa")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(val)
	db.Delete("aaa")
	val, err = db.Get("aaa")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(val)
}
