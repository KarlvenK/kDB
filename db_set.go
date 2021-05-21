package kDB

import (
	"github.com/KarlvenK/kDB/ds/set"
	"sync"
)

//SetIdx the set idx
type SetIdx struct {
	mu      sync.RWMutex
	indexes *set.Set
}

func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New()}
}
