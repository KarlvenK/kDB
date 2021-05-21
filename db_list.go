package kDB

import (
	"container/list"
	"sync"
)

// ListIdx the list idx
type ListIdx struct {
	mu      sync.RWMutex
	indexes *list.List
}
