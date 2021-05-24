package kDB

import (
	"github.com/KarlvenK/kDB/index"
	"time"
)

//DataType define the data type
type DataType = uint16

// five different data types
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// string operations
const (
	StringSet uint16 = iota
	StringRem
)

//list operations
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
)

// hash table operations
const (
	HashHSet uint16 = iota
	HashHDel
)

// set operations
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
)

// sorted set operations
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
)

//buildStringIndex build string indexes
func (db *kDB) buildStringIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	now := uint32(time.Now().Unix())
	if deadline, exist := db.expires[string(idx.Meta.Key)]; exist && deadline <= now {
		return
	}
	switch opt {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	}
}
