package cache

import (
	"container/list"
	"errors"
	"github.com/tidwall/assert"
	"github.com/tidwall/btree"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KEY interface {
	~string | ~int
}

type VALUE interface {
	~string
}

type BucketCache[K KEY, V VALUE] struct {
	shardN  uint64
	buckets []*bucket[K, V]
	hasher  Hasher[K]
	flushes int
}

type bucket[K KEY, V VALUE] struct {
	mu       sync.RWMutex
	timerMu  sync.Mutex
	keys     *btree.BTreeG[dbItem[K, V]]
	exps     *btree.BTreeG[dbItem[K, V]]
	db       *BucketCache[K, V]
	timer    *time.Timer
	listMap  map[K]*LIRList[V]
	listChan map[K]*listBroadcast
	zMaps    map[K]*zSetTable[K]
}

type zSetItem[K KEY] struct {
	score  float64
	member K
}

func zSetLess[K KEY](a, b zSetItem[K]) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return a.member < b.member
}

type ZRangeByScoreOpts struct {
	Min, Max   float64
	MinEx      bool
	MaxEx      bool
	WithScores bool
	Limit      bool
	Offset     int
	Count      int
	Reverse    bool
}

func ParseZRangeByScoreArgs(args []string) (ZRangeByScoreOpts, error) {
	opts := ZRangeByScoreOpts{Min: math.Inf(-1), Max: math.Inf(1)}
	i := 0

	if i < len(args) {
		opts.Max, opts.MaxEx = parseScore(args[i])
		i++
	}
	if i < len(args) {
		opts.Min, opts.MinEx = parseScore(args[i])
		i++
	}
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			opts.WithScores = true
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return opts, errors.New("LIMIT requires 2 arguments")
			}
			opts.Limit = true
			opts.Offset, _ = strconv.Atoi(args[i+1])
			opts.Count, _ = strconv.Atoi(args[i+2])
			i += 3
		default:
			return opts, errors.New("syntax error")
		}
	}
	return opts, nil
}

func parseScore(s string) (v float64, exclusive bool) {
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "-inf":
		return math.Inf(-1), exclusive
	case "+inf":
		return math.Inf(1), exclusive
	}
	v, _ = strconv.ParseFloat(s, 64)
	return
}

type zSetTable[K KEY] struct {
	idx  *btree.BTreeG[zSetItem[K]]
	dict map[K]*zSetItem[K]
}

type LIRList[V VALUE] struct {
	l *list.List
}

func newZSetTable[K KEY]() *zSetTable[K] {
	return &zSetTable[K]{
		idx:  btree.NewBTreeG(zSetLess[K]),
		dict: make(map[K]*zSetItem[K]),
	}
}

func NewLIRList[V VALUE]() *LIRList[V] {
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

type dbItem[K KEY, V VALUE] struct {
	key     K
	val     V
	opts    *dbItemOpts
	keyless bool
}

func (db *BucketCache[K, V]) Set(key K, val V, opts *SetOptions) error {
	item := &dbItem[K, V]{key: key, val: val}
	if opts != nil {
		if opts.Expires {
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

func (sharDB *bucket[K, V]) lPush(key K, val V) {
	sharDB.mu.Lock()
	defer sharDB.mu.Unlock()
	if l, ok := sharDB.listMap[key]; ok {
		l.LPush(val)
	} else {
		sharDB.listMap[key] = NewLIRList[V]()
		sharDB.listMap[key].LPush(val)
	}

	if _, ok := sharDB.listChan[key]; ok {
		sharDB.listChan[key].pub(1)
	} else {
		sharDB.listChan[key] = &listBroadcast{}
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
	if timeout > 0 {
		for {

			select {
			case <-ch:
				val, err := sharDB.rPop(key)
				if err == nil {
					return val, nil
				}

			case <-time.After(timeout):
				sharDB.mu.Lock()
				bc.unsub(ch)
				close(ch)
				sharDB.mu.Unlock()
				return zero, errors.New("timeout")

			}

		}
	} else {
		for {
			select {
			case <-ch:
				val, err := sharDB.rPop(key)
				if err == nil {
					return val, nil
				}
			}
		}
	}

}

func (sharDB *bucket[K, V]) set(item dbItem[K, V]) error {
	sharDB.mu.Lock()
	defer sharDB.mu.Unlock()
	sharDB.keys.Set(item)
	if item.opts != nil {

		sharDB.exps.Set(item)
		minKey, ok := sharDB.exps.Min()
		if ok {
			if !lessTimeFunc(minKey, item) {
				sharDB.scheduleTimerLocked()
			}
		} else {
			sharDB.scheduleTimerLocked()
		}

	}
	return nil
}

func (sharDB *bucket[K, V]) get(key K) (dbItem[K, V], error) {
	sharDB.mu.RLock()
	defer sharDB.mu.RUnlock()
	item, ok := sharDB.keys.Get(dbItem[K, V]{key: key})
	if ok {
		return item, nil
	} else {
		return dbItem[K, V]{key: key}, errors.New("not found")
	}
}

func (sharDB *bucket[K, V]) zAdd(key K, score float64, member K) (int, error) {
	sharDB.mu.Lock()
	defer sharDB.mu.Unlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		zt = newZSetTable[K]()
		sharDB.zMaps[key] = zt
	}

	old, exists := zt.dict[member]
	if exists {
		zt.idx.Delete(*old)
		old.score = score
		zt.idx.Set(*old)
		return 0, nil
	}

	it := &zSetItem[K]{
		score:  score,
		member: member,
	}
	zt.dict[member] = it
	zt.idx.Set(*it)
	return 1, nil

}

func (sharDB *bucket[K, V]) zRem(key K, member K) int {
	sharDB.mu.Lock()
	defer sharDB.mu.Unlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		return 0
	}
	it, ok := zt.dict[member]
	if !ok {
		return 0
	}
	zt.idx.Delete(*it)
	delete(zt.dict, member)
	if len(zt.dict) == 0 {
		delete(sharDB.zMaps, key)
	}
	return 1
}

func (sharDB *bucket[K, V]) zScore(key K, member K) (float64, bool) {
	sharDB.mu.RLock()
	defer sharDB.mu.RUnlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		return 0, false
	}
	it, ok := zt.dict[member]
	if !ok {
		return 0, false
	}
	return it.score, true
}

func (sharDB *bucket[K, V]) zCard(key K) int {
	sharDB.mu.RLock()
	defer sharDB.mu.RUnlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		return 0
	}
	return len(zt.dict)
}

func adjustIndex(idx, n int) int {
	if idx < 0 {
		idx += n
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func (sharDB *bucket[K, V]) zRange(key K, start, stop int, isReverse bool) []interface{} {
	sharDB.mu.RLock()
	defer sharDB.mu.RUnlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		return nil
	}
	n := zt.idx.Len()
	start = adjustIndex(start, n)
	stop = adjustIndex(stop, n)
	if start > stop {
		return nil
	}
	out := make([]interface{}, 0, stop-start+1)
	i := 0
	iter := zt.idx.Scan
	if isReverse {
		iter = zt.idx.Reverse
	}

	iter(func(it zSetItem[K]) bool {
		if i >= start && i <= stop {
			out = append(out, it.member)
			out = append(out, it.score)
		}
		i++
		return i <= stop
	})

	return out

}

func (sharDB *bucket[K, V]) zRangeByScoreOpts(key K, opts ZRangeByScoreOpts) []interface{} {
	sharDB.mu.RLock()
	defer sharDB.mu.RUnlock()
	zt, ok := sharDB.zMaps[key]
	if !ok {
		return nil
	}
	var out []interface{}
	n := 0
	iter := zt.idx.Scan
	if opts.Reverse {
		iter = zt.idx.Reverse
	}

	iter(func(it zSetItem[K]) bool {
		if !opts.MinEx && it.score < opts.Min {
			return true
		}
		if opts.MinEx && it.score <= opts.Min {
			return true
		}
		if !opts.MaxEx && it.score > opts.Max {
			return false
		}
		if opts.MaxEx && it.score >= opts.Max {
			return false
		}
		if opts.Limit {
			if n < opts.Offset {
				n++
				return true
			}
			if n >= opts.Offset+opts.Count {
				return false
			}
		}
		out = append(out, it.member)
		out = append(out, it.score)
		n++
		return true
	})

	return out

}

func (db *BucketCache[K, V]) zRangeByScoreOpts(key K, opts ZRangeByScoreOpts) ([]interface{}, error) {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zRangeByScoreOpts(key, opts), nil
}

func (db *BucketCache[K, V]) ZRevRangeByScore(key K, args ...string) ([]interface{}, error) {
	opts, err := ParseZRangeByScoreArgs(args)
	if err != nil {
		return nil, err
	}
	opts.Reverse = true
	return db.zRangeByScoreOpts(key, opts)
}

func (db *BucketCache[K, V]) ZRangeByScore(key K, args ...string) ([]interface{}, error) {
	opts, err := ParseZRangeByScoreArgs(args)
	if err != nil {
		return nil, err
	}
	opts.Reverse = false
	return db.zRangeByScoreOpts(key, opts)
}

func (db *BucketCache[K, V]) ZAdd(key K, score float64, member K) (int, error) {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zAdd(key, score, member)
}

func (db *BucketCache[K, V]) ZRem(key K, member K) int {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zRem(key, member)
}

func (db *BucketCache[K, V]) ZScore(key K, member K) (float64, bool) {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zScore(key, member)
}

func (db *BucketCache[K, V]) ZCard(key K) int {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zCard(key)
}

func (db *BucketCache[K, V]) ZRange(key K, start, stop int) []interface{} {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zRange(key, start, stop, false)
}

func (db *BucketCache[K, V]) ZRevRange(key K, start, stop int) []interface{} {
	idx := db.hasher.Hash(key) % db.shardN
	return db.buckets[idx].zRange(key, start, stop, true)
}

func (db *BucketCache[K, V]) clearMem() {
	for _, bucket := range db.buckets {
		bucket.mu.Lock()
		for key := range bucket.listChan {
			if len(bucket.listChan[key].subs) == 0 {
				delete(bucket.listChan, key)
			}
		}
		for key := range bucket.listMap {
			if bucket.listMap[key].l.Len() == 0 {
				delete(bucket.listMap, key)
			}
		}
		bucket.mu.Unlock()
	}
}

func (db *BucketCache[K, V]) TimerTask() {
	for {
		select {
		case <-time.After(time.Second * 60):
			db.clearMem()
		}
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

func lessFunc[K KEY, V VALUE](a, b dbItem[K, V]) bool {
	return a.key < b.key
}

func lessTimeFunc[K KEY, V VALUE](a, b dbItem[K, V]) bool {
	assert.Assert(a.opts != nil && b.opts != nil && a.opts.expires && b.opts.expires)
	return a.opts.exat.Before(b.opts.exat)
}

func NewCache[K KEY, V VALUE](shardBits int) (*BucketCache[K, V], error) {
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
			zMaps:    make(map[K]*zSetTable[K]),
		}
	}
	go buctetCache.TimerTask()
	return buctetCache, nil
}

func (db *BucketCache[K, V]) Close() error {
	for _, bucket := range db.buckets {
		bucket.stopTime()
	}
	return nil
}
