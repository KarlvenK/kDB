package kDB

import (
	"github.com/KarlvenK/kDB/index"
	"github.com/KarlvenK/kDB/storage"
	"log"
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
	if exist := db.StrExist(key); exist {
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
			df = db.activeFile[idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}
	return nil, ErrKeyNotExist
}

func (db *kDB) GetSet(key, val []byte) (res []byte, err error) {

}

func (db *kDB) Append(key, value []byte) error {

}

func (db *kDB) StrLen(key []byte) int {

}

func (db *kDB) StrExist(key []byte) bool {

}

func (db *kDB) StrRem(key []byte) error {

}

func (db *kDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {

}

func (db *kDB) RangeScan(start, end []byte) (vals [][]byte, err error) {

}

func (db *kDB) Expire(key []byte, seconds uint32) (err error) {

}

func (db *kDB) Persist(key []byte) {

}

func (db *kDB) TTL(key []byte) (ttl uint32) {

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

	return
}
