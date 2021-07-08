package kDB

import (
	"github.com/KarlvenK/kDB/ds/hash"
	"github.com/KarlvenK/kDB/storage"
	"sync"
)

//HashIdx hash idx
type HashIdx struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New()}
}

//HSet set field in the hash stored at key to value
func (db *kDB) HSet(key, field, value []byte) (res int, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, Hash, HashHSet)
	if err = db.store(e); err != nil {
		return
	}

	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}

//HSetNx set field in the hash stored at key to value
func (db *kDB) HSetNx(key, field, value []byte) (res bool, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(key), string(field), value); res {
		e := storage.NewEntry(key, value, field, Hash, HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

//HGet 返回哈希表中给定域的值
func (db *kDB) HGet(key, field []byte) []byte {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HGet(string(key), string(field))
}

//HGetAll return all fields and values of the stored at key
func (db *kDB) HGetAll(key []byte) [][]byte {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	return db.hashIndex.indexes.HGetAll(string(key))
}

//HDel remove the specified fields from the hash stored at key
func (db *kDB) HDel(key []byte, field ...[]byte) (res int, err error) {
	if field == nil || len(field) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := db.hashIndex.indexes.HDel(string(key), string(f)); ok {
			e := storage.NewEntry(key, nil, f, Hash, HashHDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}

	return
}

//HExists return if there is an existing field in the hash stored at key
func (db *kDB) HExists(key, field []byte) bool {
	if err := db.checkKeyValue(key, nil); err != nil {
		return false
	}
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HExists(string(key), string(field))
}

//HLen return the number of fields contained in the hash stored at key
func (db *kDB) HLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HLen(string(key))
}

//HKeys return all field names in the hash stored at key
func (db *kDB) HKeys(key []byte) (val []string) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HKeys(string(key))
}

//HValues return all values in the hash stored at key
func (db *kDB) HValues(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	return db.hashIndex.indexes.HValues(string(key))
}
