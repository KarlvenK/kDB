package kDB

import (
	"errors"
	"github.com/KarlvenK/kDB/index"
	"github.com/KarlvenK/kDB/storage"
	"os"
	"sync"
)

var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	// ErrCfgNotExist the config is not exist
	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	// ErrReclaimUnreached not ready to reclaim
	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("rosedb: key is expired")
)

const (

	// 保存配置的文件名称
	// rosedb config save path
	configSaveFile = string(os.PathSeparator) + "db.cfg"

	// 保存数据库相关信息的文件名称
	// rosedb meta info save path
	dbMetaSaveFile = string(os.PathSeparator) + "db.meta"

	// 回收磁盘空间时的临时目录
	// rosedb reclaim path
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// 保存过期字典的文件名称
	// expired directory save path
	expireFile = string(os.PathSeparator) + "db.expires"

	// ExtraSeparator 额外信息的分隔符，用于存储一些额外的信息（因此一些操作的value中不能包含此分隔符）
	// separator of the extra info
	ExtraSeparator = "\\0"
)

type (
	// kDB the kdb struct
	kDB struct {
		activeFile   *storage.DBFile // current active file
		activeFileID uint32          //current active file id
		archFiles    ArchivedFiles   //the archived files
		strIndex     *StrIdx         //string indexes
		listIndex    *ListIdx        //list indexes
		hashIndex    *HashIdx        //hash indexes
		setIndex     *SetIdx         //set indexes
		zsetIndex    *ZsetIdx        //Zset indexes
		config       Config          //config of kdb
		mu           sync.RWMutex
		meta         *storage.DBMeta //meta info for kdb
		expires      storage.Expires //expired directory
	}

	//ArchivedFiles define the archived files
	ArchivedFiles map[uint32]*storage.DBFile
)

func Open(config Config) (*kDB, error) {

}

func Reopen(path string) (*kDB, error) {

}

func (db *kDB) Close() error {

}

func (db *kDB) Sync() error {

}

func (db *kDB) Reclaim() (err error) {

}

func (db *kDB) Backup(dir string) (err error) {

}

func (db *kDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

func (db *kDB) saveConfig() (err error) {

}

func (db *kDB) saveMeta() error {

}

//buildIndex 建立索引
func (db *kDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {
	if db.config.IdxMode == KeyValueRamMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}

	switch e.Type {
	case storage.String:
		db.buildStringIndex(idx, e.Mark)
	case storage.List:
		db.buildListIndex(idx, e.Mark)
	case storage.Hash:
		db.buildHashIndex(idx, e.Mark)
	case storage.Set:
		db.buildSetIndex(idx, e.Mark)
	case storage.ZSet:
		db.buildZsetIndex(idx, e.Mark)
	}
	return nil
}

//store entry to db file
func (db *kDB) store(e *storage.Entry) error {
	//sync the db file if file size is not enough, and open a new db file
	config := db.config
	if db.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}

		//save the old file
		db.archFiles[db.activeFileID] = db.activeFile
		activeFileID := db.activeFileID + 1

		if dbFile, err := storage.NewDBFile(config.DirPath, activeFileID, config.RwMethod, config.BlockSize); err != nil {
			return err
		} else {
			db.activeFile = dbFile
			db.activeFileID = activeFileID
			db.meta.ActiveWriteOff = 0
		}
	}

	//write data to db file
	if err := db.activeFile.Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff = db.activeFile.Offset

	//persist the data to disk
	if config.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *kDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {

}
