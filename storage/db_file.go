package storage

import (
	"errors"
	"os"
)

const (
	// FilePerm 默认的创建文件权限 permission
	FilePerm = 0644

	// DBFileFormatName default数据文件名称格式化
	DBFileFormatName = "%09d.data"

	//PathSeparator the default path separator
	PathSeparator = string(os.PathSeparator)
)

var (
	// ErrEmptyEntry the entry is empty
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

//FileRWMethod 文件数据读写方式
type FileRWMethod uint8

const (
	// FileIO 表示文件数据读写使用系统标准IO
	FileIO FileRWMethod = iota

	// MMap 表示文件数据读写使用Mmap
	// MMap 指的是将文件或其他设备映射至内存 via https://en.wikipedia.org/wiki/Mmap
	MMap
)
