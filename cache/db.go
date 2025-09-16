package cache

import (
	"container/list"
	"errors"
	"github.com/tidwall/assert"
	"github.com/tidwall/btree"
	"sync"
	"time"
)

type KEY interface {
	~string | ~int
}

type BucketCache[K KEY, V any] struct {
	shardN  uint64
	buckets []*bucket[K, V]
	hasher  Hasher[K]
	flushes int
}

type bucket[K KEY, V any] struct {
	mu       sync.RWMutex
	timerMu  sync.Mutex
	keys     *btree.BTreeG[dbItem[K, V]]
	exps     *btree.BTreeG[dbItem[K, V]]
	db       *BucketCache[K, V]
	timer    *time.Timer
	listMap  map[K]*LIRList[V]
	listChan map[K]*listBroadcast
}

//type listNode[V any] struct {
//	val  V
//	next *listNode[V]
//	len  int
//}

type LIRList[V any] struct {
	l *list.List
}

func NewLIRList[V any]() *LIRList[V] {
	return &LIRList[V]{l: list.New()}
}

func (l *LIRList[V]) LPush(val V) {
	l.l.PushFront(val)
}

func (l *LIRList[V]) RPop() (V, error) {
	var zero V
	back := l.l.Back()
	if back == nil {
		return zero, errors.New("list is empty")
	}
	return l.l.Remove(back).(V), nil
}

type listBroadcast struct {
	mu   sync.Mutex
	subs map[chan int]struct{}
	//order []chan int
}

func (b *listBroadcast) sub() chan int {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan int)
	b.subs[ch] = struct{}{}
	return ch
}

func (b *listBroadcast) pub(v int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- v:
		default:
		}
	}
}

func (b *listBroadcast) unsub(ch chan int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, ch)
}

func (b *bucket[K, V]) scheduleTimerLocked() {
	minKey, ok := b.exps.Min()
	if !ok {
		b.stopTime()
		return
	}

	next := time.Until(minKey.opts.exat)
	if next < 0 {
		b.stopTime()
		go b.evictExpired()
		return
	}
	b.resetTimer(next)
}

// dd
func (b *bucket[K, V]) resetTimer(d time.Duration) {
	b.timerMu.Lock()
	defer b.timerMu.Unlock()
	if b.timer != nil {
		b.timer.Reset(d)
	} else {
		b.timer = time.AfterFunc(d, b.onTime)
	}
}

func (b *bucket[K, V]) onTime() {

	b.evictExpired()
}

func (b *bucket[K, V]) delete(key K) {
	b.mu.Lock()
	defer b.mu.Unlock()

	val, ok := b.keys.Delete(dbItem[K, V]{key: key})
	if ok == false {
		return
	}
	if val.opts != nil && val.opts.expires {
		b.exps.Delete(val)
	}

}

func (b *bucket[K, V]) evictExpired() {
	defer b.scheduleTimerLocked()
	now := time.Now()
	for {
		minKey, ok := b.exps.Min()
		if !ok || now.Before(minKey.opts.exat) {
			break
		}
		b.delete(minKey.key)
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
	expires bool
	exat    time.Time
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
			item.opts = &dbItemOpts{expires: true, exat: time.Now().Add(opts.TTL)}
		}

	}
	index := db.hasher.Hash(key) % db.shardN
	shard := db.buckets[index]
	err := shard.set(*item)
	return err

}

func (db *BucketCache[K, V]) Delete(key K) error {
	index := db.hasher.Hash(key) % db.shardN
	shard := db.buckets[index]
	shard.delete(key)
	return nil
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

func (db *BucketCache[K, V]) LPush(key K, val V) {
	idx := db.hasher.Hash(key) % db.shardN
	bucket := db.buckets[idx]
	bucket.lPush(key, val)
}

func (db *BucketCache[K, V]) LLen(key K) int {
	idx := db.hasher.Hash(key) % db.shardN
	bucket := db.buckets[idx]
	return bucket.lLen(key)

}

func (db *BucketCache[K, V]) BRPop(key K, timeout time.Duration) (val V, err error) {
	idx := db.hasher.Hash(key) % db.shardN
	bucket := db.buckets[idx]
	return bucket.brPop(key, timeout)
}

func (db *BucketCache[K, V]) RPop(key K) (val V, err error) {
	idx := db.hasher.Hash(key) % db.shardN
	bucket := db.buckets[idx]
	return bucket.rPop(key)
}

func (sharDb *bucket[K, V]) lPush(key K, val V) {
	sharDb.mu.Lock()
	defer sharDb.mu.Unlock()
	if l, ok := sharDb.listMap[key]; ok {
		l.LPush(val)
	} else {
		sharDb.listMap[key] = NewLIRList[V]()
		sharDb.listMap[key].LPush(val)
	}

	if _, ok := sharDb.listChan[key]; ok {
		sharDb.listChan[key].pub(1)
	} else {
		sharDb.listChan[key] = &listBroadcast{}
	}

}

func (sharDB *bucket[K, V]) rPop(key K) (val V, err error) {
	sharDB.mu.Lock()
	defer sharDB.mu.Unlock()
	var zero V
	if l, ok := sharDB.listMap[key]; ok {
		val, err := l.RPop()
		if err != nil {
			return zero, err
		}
		return val, nil
	}
	return zero, errors.New("key not found")
}

func (b *bucket[K, V]) lLen(key K) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	l, ok := b.listMap[key]
	if !ok {
		return 0
	}
	return l.l.Len()
}

func (sharDB *bucket[K, V]) brPop(key K, timeout time.Duration) (val V, err error) {
	val, err = sharDB.rPop(key)

	if err == nil {
		return val, nil
	}

	sharDB.mu.Lock()
	if _, ok := sharDB.listChan[key]; !ok {
		bc := &listBroadcast{subs: map[chan int]struct{}{}}
		sharDB.listChan[key] = bc
	}
	bc := sharDB.listChan[key]
	ch := bc.sub()
	sharDB.mu.Unlock()
	var zero V
	for {

		select {
		case <-ch:
			val, err := sharDB.rPop(key)
			if err == nil {
				return val, nil
			}

		case <-time.After(timeout):
			bc.unsub(ch)
			close(ch)
			return zero, errors.New("timeout")

		}

	}
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
	assert.Assert(a.opts != nil && b.opts != nil && a.opts.expires && b.opts.expires)
	return a.opts.exat.Before(b.opts.exat)
}

func NewCache[K KEY, V any](shardBits int) (*BucketCache[K, V], error) {
	if shardBits < 0 || shardBits > 8 {
		return nil, errors.New("shardBits should in [0,8]")
	}
	shardNum := 1 << shardBits
	buctetCache := &BucketCache[K, V]{
		buckets: make([]*bucket[K, V], shardNum),
		hasher:  defaultHasher[K](),
		shardN:  uint64(shardNum),
	}

	for i := 0; i < shardNum; i++ {
		buctetCache.buckets[i] = &bucket[K, V]{
			mu:       sync.RWMutex{},
			keys:     btree.NewBTreeG(lessFunc[K, V]),
			exps:     btree.NewBTreeG(lessTimeFunc[K, V]),
			db:       buctetCache,
			listMap:  make(map[K]*LIRList[V]),
			listChan: make(map[K]*listBroadcast),
		}
	}

	return buctetCache, nil
}

func (db *BucketCache[K, V]) Close() error {
	for _, bucket := range db.buckets {
		bucket.stopTime()
	}
	return nil
}
