package kDB

import "github.com/KarlvenK/kDB/storage"

// DataIndexMode 数据索引模式
type DataIndexMode int

const (
	// KeyValueRamMode 键和值均存在于内存中的模式
	KeyValueRamMode DataIndexMode = iota

	// KeyOnlyRamMode 只有键存在于内存中的模式
	KeyOnlyRamMode
)

const (
	//DefaultAddr default kdb server address
	DefaultAddr = "127.0.0.1:5200"

	// DefaultDirPath kdb data dir
	DefaultDirPath = "/tmp/kdb_server"

	//DefaultBlockSize db file size : 16MB
	DefaultBlockSize = 16 * 1024 * 1024

	// DefaultMaxKeySize 默认的key最大值 128字节
	// default max key size: 128 bytes
	DefaultMaxKeySize = uint32(128)

	// DefaultMaxValueSize 默认的value最大值 1MB
	// default max value size: 1mb
	DefaultMaxValueSize = uint32(1 * 1024 * 1024)

	// DefaultReclaimThreshold 默认回收磁盘空间的阈值，当已封存文件个数到达 4 时，可进行回收
	// default disk reclaim threshold: 4
	DefaultReclaimThreshold = 4
)

// Config 数据库配置
// the config options of kdb
type Config struct {
	Addr             string               `json:"addr" toml:"addr"`             //服务器地址          server address
	DirPath          string               `json:"dir_path" toml:"dir_path"`     //数据库数据存储目录   kdb dir path of db file
	BlockSize        int64                `json:"block_size" toml:"block_size"` //每个数据块文件的大小 each db file size
	RwMethod         storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   //数据读写模式        db file read and write method
	IdxMode          DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     //数据索引模式        data index mode
	MaxKeySize       uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize     uint32               `json:"max_value_size" toml:"max_value_size"`
	Sync             bool                 `json:"sync" toml:"sync"`                           //每次写数据是否持久化 sync to disk
	ReclaimThreshold int                  `json:"reclaim_threshold" toml:"reclaim_threshold"` //回收磁盘空间的阈值   threshold to reclaim disk
}

// DefaultConfig 获取默认配置
func DefaultConfig() Config {
	return Config{
		Addr:             DefaultAddr,
		DirPath:          DefaultDirPath,
		BlockSize:        DefaultBlockSize,
		RwMethod:         storage.FileIO,
		IdxMode:          KeyValueRamMode,
		MaxKeySize:       DefaultMaxKeySize,
		MaxValueSize:     DefaultMaxValueSize,
		Sync:             false,
		ReclaimThreshold: DefaultReclaimThreshold,
	}
}
