package kDB

import (
	"github.com/KarlvenK/kDB/ds/list"
	"github.com/KarlvenK/kDB/storage"
	"log"
	"strconv"
	"sync"
)

// ListIdx the list idx
type ListIdx struct {
	mu      sync.RWMutex
	indexes *list.List
}

func newList() *ListIdx {
	return &ListIdx{indexes: list.New()}
}

// LPush insert all the specified values at the head of the list stored at key
// if key dose not exist, it is created as empty list before performing the push operation
func (db *kDB) LPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.LPush(string(key), val)
	}
	return
}

//RPush insert all the specified values ast the tail of the list at key
//if key does not exist, it is created as empty list before performing operation
func (db *kDB) RPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.RPush(string(key), val)
	}

	return
}

//LPop remove and return the first element if the list stored at key
func (db *kDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.LPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListLPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when ListLPop data")
		}
	}

	return val, nil
}

//RPop remove and return the last element of the list stored at key
func (db *kDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val := db.listIndex.indexes.RPop(string(key))

	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListRPop)
		if err := db.store(e); err != nil {
			log.Println("error occurred when store ListRPop data")
		}
	}

	return val, nil
}

//LIndex 返回列表在index处的值，如果不存在则返回nil
//return the element at index index in the list stored at key
//the index is zero-based, so 0 means the first element, 1 the second element and so on
//negative indices can be used to designate elements starting at the tail of the list
func (db *kDB) LIndex(key []byte, idx int) []byte {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LIndex(string(key), idx)
}

//LRem 根据count的绝对值， 移除列表中与参数 value相等的元素
//count > 0: remove elements equal to element moving from head to tail
//count < 0: remove elements equal to element moving from tail to head
//count = 0: remove all elements equal to element
func (db *kDB) LRem(key, value []byte, count int) (int, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	res := db.listIndex.indexes.LRem(string(key), value, count)

	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		if err := db.store(e); err != nil {
			return res, err
		}
	}

	return res, nil
}
