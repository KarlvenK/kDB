package kDB

import (
	"github.com/KarlvenK/kDB/index"
	"sync"
)

//StrIdx string idx
type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}
