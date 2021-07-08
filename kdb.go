package kDB

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarlvenK/kDB/index"
	"github.com/KarlvenK/kDB/storage"
	"github.com/KarlvenK/kDB/utils"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
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

//Open 打开一个数据库实例
func Open(config Config) (*kDB, error) {
	//create the dirs if not it exists
	if utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//load the db files
	archFiles, activeFileId, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//load expired directories
	expires := storage.LoadExpires(config.DirPath + expireFile)

	//load db meta info
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	activeFile.Offset = meta.ActiveWriteOff

	db := &kDB{
		activeFile:   activeFile,
		activeFileID: activeFileId,
		archFiles:    archFiles,
		config:       config,
		strIndex:     newStrIdx(),
		meta:         meta,
		listIndex:    newList(),
		hashIndex:    newHashIdx(),
		setIndex:     newSetIdx(),
		zsetIndex:    newZsetIdx(),
		expires:      expires,
	}

	//load indexers from files
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func Reopen(path string) (*kDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config
	bytes, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

//Close 关闭数据库，保存config
func (db *kDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}
	if err := db.expires.SaveExpires(db.config.DirPath + expireFile); err != nil {
		return err
	}

	for _, archFile := range db.archFiles {
		if err := archFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

//Sync 数据持久化
func (db *kDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.activeFile.Sync()
}

//Reclaim 重新组织磁盘中的数据，回收磁盘空间
func (db *kDB) Reclaim() (err error) {
	if len(db.archFiles) < db.config.ReclaimThreshold {
		return ErrReclaimUnreached
	}

	//新建临时目录， 用于暂存新的数据文件
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	var (
		activeFileId uint32 = 0
		newArchFiles        = make(ArchivedFiles)
		df           *storage.DBFile
	)

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, file := range db.archFiles {
		var offset int64 = 0
		var reclaimEntries []*storage.Entry

		var dfFile *os.File
		dfFile, err = os.Open(file.File.Name())
		if err != nil {
			return err
		}
		file.File = dfFile
		fileId := file.Id

		for {
			if e, err := file.Read(offset); err == nil {
				//check if the entry is valid
				if db.validEntry(e, offset, fileId) {
					reclaimEntries = append(reclaimEntries, e)
				}
				offset += int64(e.Size())
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}

		//rewrite entry to the db file
		if len(reclaimEntries) > 0 {
			for _, entry := range reclaimEntries {
				if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
					df, err := storage.NewDBFile(reclaimPath, activeFileId, db.config.RwMethod, db.config.BlockSize)
					if err != nil {
						return err
					}

					newArchFiles[activeFileId] = df
					activeFileId++
				}

				if err = df.Write(entry); err != nil {
					return
				}

				//update string indexers
				if entry.Type == String {
					item := db.strIndex.idxList.Get(entry.Meta.Key)
					idx := item.Value().(*index.Indexer)
					idx.Offset = df.Offset - int64(entry.Size())
					idx.FileId = activeFileId
					db.strIndex.idxList.Put(idx.Meta.Key, idx)
				}
			}
		}
	}
	//删除旧的数据，临时目录拷贝位新的数据文件
	//delete the old db files, and copy the directory as new db files
	for _, v := range db.archFiles {
		_ = os.Remove(v.File.Name())
	}

	for _, v := range newArchFiles {
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatName, v.Id)
		os.Rename(reclaimPath+name, db.config.DirPath+name)
	}

	db.archFiles = newArchFiles
	return
}

//Backup 复制数据库目录，用于备份
func (db *kDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyFile(db.config.DirPath, dir)
	}
	return
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

//saveConfig 关闭数据库之前保存配置
func (db *kDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	bytes, err := json.Marshal(db.config)
	_, err = file.Write(bytes)
	err = file.Close()
	return
}

func (db *kDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
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

//validEntry 判断entry所属的操作标识（增、改类型操作），以及val是否有效
func (db *kDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}
	mark := e.Mark
	switch e.Type {
	case String:
		if mark == StringSet {
			// expired key is invalid
			now := uint32(time.Now().Unix())
			if deadline, exist := db.expires[string(e.Meta.Key)]; exist && deadline < now {
				return false
			}

			//check the data position
			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer == nil || indexer.FileId != fileId || indexer.Offset != offset {
					return false
				}
			}

			if val, err := db.Get(e.Meta.Key); err == nil && string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case List:
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			return true
		}
	case Hash:
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}
		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val {
					return true
				}
			}
		}
	}
	return false
}
