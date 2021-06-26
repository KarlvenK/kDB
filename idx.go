package kDB

import (
	"github.com/KarlvenK/kDB/ds/list"
	"github.com/KarlvenK/kDB/index"
	"github.com/KarlvenK/kDB/utils"
	"io"
	"sort"
	"strconv"
	"strings"
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

// buildListIndex build list indexes
func (db *kDB) buildListIndex(idx *index.Indexer, opt uint16) {
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ListLPush:
		db.listIndex.indexes.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(idx.Meta.Key), start, end)
		}
	}
}

//buildHashIndex build hash indexes
func (db *kDB) buildHashIndex(idx *index.Indexer, opt uint16) {
	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)

	switch opt {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(idx.Meta.Extra))
	}
}

// buildSetIndex 建立集合索引
// build set indexes
func (db *kDB) buildSetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	}
}

// buildZsetIndex 建立有序集合索引
// build sorted set indexes
func (db *kDB) buildZsetIndex(idx *index.Indexer, opt uint16) {

	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch opt {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	}
}

//loadIdxFromFiles load String、List、Hash、Set、ZSet indexes from files
func (db *kDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	var fileIds []int
	dbFile := make(ArchivedFiles)
	for k, v := range db.archFiles {
		dbFile[k] = v
		fileIds = append(fileIds, int(k))
	}

	sort.Ints(fileIds)
	for i := 0; i < len(fileIds); i++ {
		fid := uint32(fileIds[i])
		df := dbFile[fid]
		var offset int64 = 0

		for offset <= db.config.BlockSize {
			if e, err := df.Read(offset); err == nil {
				idx := &index.Indexer{
					Meta:      e.Meta,
					FileId:    fid,
					EntrySize: e.Size(),
					Offset:    offset,
				}
				offset += int64(e.Size())

				if err := db.buildIndex(e, idx); err != nil {
					return err
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}

	return nil
}
