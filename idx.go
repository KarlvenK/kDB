package kDB

//DataType define the data type
type DataType = uint16

// five diferent data types
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// string operations
const (
	StringSet uint16 = iota
	StringRem
)

//list operations
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
)

//hash table operations
const (
	HashHset uint16 = iota
	HashHDel
)

// set operations
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
)

// sorted set operations
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
)
