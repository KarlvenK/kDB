package kDB

import (
	"github.com/KarlvenK/kDB/ds/zset"
	"sync"
)

//ZsetIdx the zset idx
type ZsetIdx struct {
	mu      sync.RWMutex
	indexes *zset.SortedSet
}
