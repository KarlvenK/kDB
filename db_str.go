package kDB

import (
	"bytes"
	"github.com/KarlvenK/kDB/index"
	"github.com/KarlvenK/kDB/storage"
	"log"
	"strings"
	"sync"
	"time"
)

//StrIdx string idx
type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{idxList: index.NewSkipList()}
}

//Set set key to hold the string value
//if key already holds a value, it is overwritten
func (db *kDB) Set(key, value []byte) error {
	if err := db.doSet(key, value); err != nil {
		return err
	}

	db.Persist(key)
	return nil
}

//SetNx 是SET if not exists 的缩写
// 只在key不存在的情况下， 将key的值设置为value
// 所key已经存在则不进行任何操作
func (db *kDB) SetNx(key, value []byte) error {
	if exist := db.StrExists(key); exist {
		return nil
	}

	return db.Set(key, value)
}

//Get get the value of key, if the key does not exist return an error
func (db *kDB) Get(key []byte) ([]byte, error) {
	ketSize := uint32(len(key))
	if ketSize == 0 {
		return nil, ErrEmptyKey
	}

	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}
	//Value returns interface{}
	//change it into Indexer
	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	//check if key is expired
	if db.expireIfNeeded(key) {
		return nil, ErrKeyExpired
	}

	if db.config.IdxMode == KeyValueRamMode {
		return idx.Meta.Value, nil
	}

	if db.config.IdxMode == KeyOnlyRamMode {
		df := db.activeFile
		if idx.FileId != db.activeFileID {
			df = db.archFiles[idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}
	return nil, ErrKeyNotExist
}

//GetSet 将key的值设置味value， 并返回key在设置前的旧value
func (db *kDB) GetSet(key, val []byte) (res []byte, err error) {
	if res, err = db.Get(key); err != nil {
		return
	}
	if err = db.Set(key, val); err != nil {
		return
	}
	return
}

//Append 如果key存在， 将 value追加到原来的value末尾
//key不存在，则相当于Set方法
func (db *kDB) Append(key, value []byte) error {
	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}
	e, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}
	if db.expireIfNeeded(key) {
		return ErrKeyExpired
	}

	appendExist := false
	if e != nil {
		appendExist = true
		e = append(e, value...)
	} else {
		e = value
	}

	if err := db.doSet(key, e); err != nil {
		return err
	}
	if !appendExist {
		db.Persist(key)
	}
	return nil
}

//StrLen return the length of the string value stored at key
func (db *kDB) StrLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.Get(key)
	if e != nil {
		if db.expireIfNeeded(key) {
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}
	return 0
}

// StrExists check whether the key exists
func (db *kDB) StrExists(key []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.expireIfNeeded(key) {
		return true
	}
	return false
}

//StrRem remove the value stored at key
func (db *kDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if ele := db.strIndex.idxList.Remove(key); ele != nil {
		delete(db.expires, string(key))
		e := storage.NewEntryNoExtra(key, nil, String, StringRem)
		if err := db.store(e); err != nil {
			return err
		}
	}

	return nil
}

//PrefixScan 根据前缀查找所有匹配的 key 对应的 value
//limit 和 offset 控制取数据的范围，类似关系型数据库的分页操作
//若 limit为负数，返回所有满足条件的结果
func (db *kDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {
	if limit == 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}
	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Indexer)
		var value []byte

		if db.config.IdxMode == KeyOnlyRamMode {
			value, err = db.Get(e.Key())
			if err != nil {
				return
			}
		} else {
			if item != nil {
				value = item.Meta.Value
			}
		}

		expired := db.expireIfNeeded(e.Key())
		if !expired {
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired {
			limit--
		}
	}
	return
}

//RangeScan 范围扫描， 查找key 从start 到 end之间的数据
func (db *kDB) RangeScan(start, end []byte) (vals [][]byte, err error) {
	node := db.strIndex.idxList.Get(start)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), end) <= 0 {
		if db.expireIfNeeded(node.Key()) {
			node = node.Next()
			continue
		}

		var value []byte
		if db.config.IdxMode == KeyOnlyRamMode {
			value, err = db.Get(node.Key())
			if err != nil {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Indexer).Meta.Value
		}
		vals = append(vals, value)
		node = node.Next()
	}
	return
}

//Expire set the expiration time of the key
func (db *kDB) Expire(key []byte, seconds uint32) (err error) {
	if exist := db.StrExists(key); !exist {
		return ErrKeyNotExist
	}
	if seconds <= 0 {
		return ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := uint32(time.Now().Unix()) + seconds
	db.expires[string(key)] = deadline
	return
}

//Persist 清除key的过期时间
func (db *kDB) Persist(key []byte) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	delete(db.expires, string(key))
}

//TTL 获取key的过期时间
func (db *kDB) TTL(key []byte) (ttl uint32) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if db.expireIfNeeded(key) {
		return
	}

	deadline, exist := db.expires[string(key)]
	if !exist {
		return
	}
	now := uint32(time.Now().Unix())
	if deadline > now {
		ttl = deadline - now
	}
	return
}

//expireIfNeeded
//check whether key is expired and delete it
func (db *kDB) expireIfNeeded(key []byte) (expired bool) {
	deadline := db.expires[string(key)]
	if deadline <= 0 {
		return
	}

	if time.Now().Unix() > int64(deadline) {
		expired = true
		// 删除过期字典对应的key
		delete(db.expires, string(key))

		if ele := db.strIndex.idxList.Remove(key); ele != nil {
			e := storage.NewEntryNoExtra(key, nil, String, StringRem)
			if err := db.store(e); err != nil {
				log.Printf("remove expired key err [%+v] [%+v]\n", key, err)
			}
		}
	}
	return
}

func (db *kDB) doSet(key, value []byte) (err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}
	//如果新增的value和设置的value一样 则不操作
	if db.config.IdxMode == KeyValueRamMode {
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	//数据索引
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.activeFileID,
		EntrySize: e.Size(),
		Offset:    db.activeFile.Offset - int64(e.Size()),
	}
	if err = db.buildIndex(e, idx); err != nil {
		return err
	}
	return
}
