package cache

import (
	"hash/fnv"
)

type Hasher[K comparable] interface {
	Hash(key K) uint64
	Equal(a, b K) bool
}

type strHasher struct{}

func (strHasher) Hash(k string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(k))
	return h.Sum64()
}
func (strHasher) Equal(a, b string) bool { return a == b }

// ---------- 整数 ----------
type intHasher struct{}

func (intHasher) Hash(k int) uint64   { return uint64(k) }
func (intHasher) Equal(a, b int) bool { return a == b }
