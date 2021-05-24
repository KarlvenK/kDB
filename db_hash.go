package kDB

import (
	"github.com/KarlvenK/kDB/ds/hash"
	"sync"
)

//HashIdx hash idx
type HashIdx struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New()}
}