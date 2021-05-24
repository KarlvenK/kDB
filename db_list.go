package kDB

import (
	"github.com/KarlvenK/kDB/ds/list"
	"github.com/KarlvenK/kDB/storage"
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
