package cache

import (
	"errors"
	"github.com/tidwall/btree"
	"os"
	"sync"
	"time"
)

// TODO 加入channel btree， 实现异步加减法
type KEY interface {
	~string | ~int
}

type BucketCache[K KEY, V any] struct {
	shardN  uint64
	aof     *os.File
	buf     []byte
	buckets []*bucket[K, V]
	aofC    chan []byte
	hasher  Hasher[K]
	flushes int
}

type bucket[K KEY, V any] struct {
	mu      sync.RWMutex
	aofC    chan []byte
	keys    *btree.BTreeG[dbItem[K, V]]
	exps    *btree.BTreeG[dbItem[K, V]]
	db      *BucketCache[K, V]
	timer   *time.Timer
	timerMu sync.Mutex
}

func (b *bucket[K, V]) scheduleTimerLocked() {
	minKey, ok := b.exps.Min()
	if !ok {
		b.stopTime()
		return
	}

	next := time.Until(minKey.opts.exat)
	if next < 0 {
		go b.evictExpired()
		return
	}
	b.resetTimer(next)
}

// dd
func (b *bucket[K, V]) resetTimer(d time.Duration) {
	b.timerMu.Lock()
	b.timerMu.Unlock()
	if b.timer != nil {
		b.timer.Reset(d)
	} else {
		b.timer = time.AfterFunc(d, b.onTime)
	}
}

func (b *bucket[K, V]) onTime() {

	b.evictExpired()
}

func (b *bucket[K, V]) Delete(key K) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.keys.Delete(key)
	b.exps.Delete(key)
}

func (b *bucket[K, V]) evictExpired() {
	defer b.scheduleTimerLocked()
	now := time.Now()
	for {
		minKey, ok := b.exps.Min()
		if !ok || now.Before(minKey.opts.exat) {
			break
		}
		b.mu.Lock()
		b.keys.Delete(minKey)
		b.exps.Delete(minKey)
		b.mu.Unlock()
	}
}

func (b *bucket[K, V]) stopTime() {
	b.timerMu.Lock()
	defer b.timerMu.Unlock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}

type dbItemOpts struct {
	exat time.Time
}

type SetOptions struct {
	Expires bool
	TTL     time.Duration
}

type dbItem[K KEY, V any] struct {
	key     K
	val     V
	opts    *dbItemOpts
	keyless bool
}

func (db *BucketCache[K, V]) Set(key K, val V, opts *SetOptions) error {
	//index :=db.hasher.Hash(key)
	item := &dbItem[K, V]{key: key, val: val}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{exat: time.Now().Add(opts.TTL)}
		}

	}
	index := db.hasher.Hash(key) % db.shardN
	shard := db.buckets[index]
	err := shard.set(*item)
	return err

}

func (db *BucketCache[K, V]) Get(key K) (val V, err error) {
	index := db.hasher.Hash(key) % db.shardN
	shard := db.buckets[index]
	dbItem, err := shard.get(key)
	if err != nil {
		var zero V
		return zero, err
	}
	return dbItem.val, nil
}

func (sharDb *bucket[K, V]) set(item dbItem[K, V]) error {
	sharDb.mu.Lock()
	defer sharDb.mu.Unlock()
	sharDb.keys.Set(item)
	if item.opts != nil {

		sharDb.exps.Set(item)
		minKey, ok := sharDb.exps.Min()
		if ok {
			if !lessTimeFunc(minKey, item) {
				sharDb.scheduleTimerLocked()
			}
		} else {
			sharDb.scheduleTimerLocked()
		}

	}
	return nil
}

func (sharDb *bucket[K, V]) get(key K) (dbItem[K, V], error) {
	sharDb.mu.RLock()
	defer sharDb.mu.RUnlock()
	item, ok := sharDb.keys.Get(dbItem[K, V]{key: key})
	if ok {
		return item, nil
	} else {
		return dbItem[K, V]{key: key}, errors.New("not found")
	}
}

func defaultHasher[K KEY]() Hasher[K] {
	var zero K
	switch any(zero).(type) {
	case string:
		return any(strHasher{}).(Hasher[K])
	case int:
		return any(intHasher{}).(Hasher[K])
	default:
		panic("no default Hasher for this key type; use NewCacheWithHasher")
	}
}

func lessFunc[K KEY, V any](a, b dbItem[K, V]) bool {
	return a.key < b.key
}

func lessTimeFunc[K KEY, V any](a, b dbItem[K, V]) bool {
	return a.opts.exat.Before(b.opts.exat)
}

func NewCache[K KEY, V any](shardBits int) (*BucketCache[K, V], error) {
	if shardBits < 0 || shardBits > 8 {
		return nil, errors.New("shardBits should in [0,8]")
	}
	shardNum := 1 << shardBits
	buctetCache := &BucketCache[K, V]{
		aof:     nil,
		buf:     make([]byte, 0, 64*1024), // 64KB 初始缓冲
		buckets: make([]*bucket[K, V], shardNum),
		aofC:    make(chan []byte, shardNum), // 足够缓冲
		hasher:  defaultHasher[K](),
		shardN:  uint64(shardNum),
	}

	for i := 0; i < shardNum; i++ {
		buctetCache.buckets[i] = &bucket[K, V]{
			mu:   sync.RWMutex{},
			aofC: make(chan []byte, shardNum),
			keys: btree.NewBTreeG(lessFunc[K, V]),
			exps: btree.NewBTreeG(lessTimeFunc[K, V]),
			db:   buctetCache,
		}
	}

	return buctetCache, nil
}

func (db *BucketCache[K, V]) Close() error {
	for _, bucket := range db.buckets {
		bucket.stopTime()
	}
	if db.aofC != nil {
		close(db.aofC)
	}
	return nil
}
