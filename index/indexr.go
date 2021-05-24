package index

import "github.com/KarlvenK/kDB/storage"

type Indexer struct {
	Meta      *storage.Meta //元数据信息  metadata info
	FileId    uint32        //存储数据的文件id 	the file id of storing the data
	EntrySize uint32        //数据条目entry的大小 the size of entry
	Offset    int64         //entry数据的查询起始位置    entry data query start position
}
