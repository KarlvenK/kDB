package kDB

import (
	"errors"
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
