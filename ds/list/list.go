package list

import (
	"container/list"
	"reflect"
)

//InsertOption insert option for LInsert
type InsertOption uint8

// insert direction
const (
	Before InsertOption = iota
	After
)

type (
	Record map[string]*list.List
	List   struct {
		record Record
	}
)

func New() *List {
	return &List{
		make(Record),
	}
}

//LPush push element in the front of list
func (myList *List) LPush(key string, val ...[]byte) int {
	return myList.push(true, key, val...)
}

func (myList *List) LPop(key string) []byte {
	return myList.pop(true, key)
}

func (myList *List) RPush(key string, val ...[]byte) int {
	return myList.push(false, key, val...)
}

func (myList *List) RPop(key string) []byte {
	return myList.pop(false, key)
}

//LIndex return list 在index处的值，不存在就return nil
func (myList *List) LIndex(key string, index int) []byte {
	ok, newIndex := myList.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex

	var val []byte
	e := myList.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}
	return val
}

// LRem 根据count的值，remove和val相等的值
// count ＞　０，　从表头->表尾 搜索，remove value相等的元素，数量为count
// count ＜　０，从表尾->表头 搜索， remove value相等的元素， 数量为 -count
//count = 0  ，移除 所有 和value相等的元素
//return the number of removed elements

func (myList *List) LRem(key string, val []byte, count int) int {
	item := myList.record[key]
	if item == nil {
		return 0
	}

	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	if count < 0 {
		for p := item.Back(); p != nil && len(ele) < (-count); p = p.Prev() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele {
		item.Remove(e)
	}
	length := len(ele)
	ele = nil
	return length
}

//LInsert insert val to list of key, in the front or back of pivot
//如果命令成功返回插入后列表的长度， 如果没有找到pivot，返回 -1
func (myList *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := myList.find(key, pivot)
	if e == nil {
		return -1
	}

	item := myList.record[key]

	switch option {
	case Before:
		item.InsertBefore(val, e)
	case After:
		item.InsertAfter(val, e)
	}

	return item.Len()
}

//LSet change the element of index to val
func (myList *List) LSet(key string, index int, val []byte) bool {
	e := myList.index(key, index)
	if e == nil {
		return false
	}

	e.Value = val
	return true
}

//LRange return list of key 中指定区间的元素，
//如果start下标比最大下标还大， 则return空列表
//如果end下标比len大，则将end修改为len - 1
func (myList *List) LRange(key string, start, end int) (values [][]byte) {
	item := myList.record[key]

	if item == nil || item.Len() <= 0 {
		return
	}
	length := item.Len()
	start, end = myList.handleIndex(length, start, end)

	mid := length >> 1

	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				values = append(values, p.Value.([]byte))
			}
		}
	} else {
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				values = append(values, p.Value.([]byte))
			}
		}

		if len(values) > 0 {
			for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
				values[i], values[j] = values[j], values[i]
			}
		}
	}

	return
}

//LTrim对一个列表进行修剪（trim),让列表值保留指定区间内的元素，不在指定区间内的元素都将被删除

func (myList *List) LTrim(key string, start, end int) bool {
	item := myList.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = myList.handleIndex(length, start, end)

	if start <= 0 && end >= length-1 {
		return false
	}
	//look below
	if start > end || start >= length {
		myList.record[key] = nil
		return true
	}

	startEle, endEle := myList.index(key, start), myList.index(key, end)

	if end-start+1 < (length >> 1) {
		newList := list.New()
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
		}

		item = nil
		myList.record[key] = newList
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev() {
			ele = append(ele, p)
		}
		for _, v := range ele {
			item.Remove(v)
		}
		ele = nil
	}
	return true
}

// LLen 返回key的list的元素个数
func (myList *List) LLen(key string) int {
	length := 0
	if myList.record[key] != nil {
		length = myList.record[key].Len()
	}
	return length
}

func (myList *List) find(key string, val []byte) *list.Element {
	item := myList.record[key]
	var e *list.Element

	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				e = p
				break
			}
		}
	}

	return e
}

func (myList *List) index(key string, index int) *list.Element {
	ok, newIndex := myList.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	item := myList.record[key]
	var e *list.Element

	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}
	return e
}

func (myList *List) push(pushFront bool, key string, val ...[]byte) int {
	if myList.record[key] == nil {
		myList.record[key] = list.New()
	}

	for _, v := range val {
		if pushFront {
			myList.record[key].PushFront(v)
		} else {
			myList.record[key].PushBack(v)
		}
	}

	return myList.record[key].Len()
}

func (myList *List) pop(popFront bool, key string) (value []byte) {
	item := myList.record[key]

	if item != nil && item.Len() > 0 {
		var e *list.Element

		if popFront {
			e = item.Front()
		} else {
			e = item.Back()
		}

		value = e.Value.([]byte)
		item.Remove(e)
	}

	return
}

func (myList *List) validIndex(key string, index int) (bool, int) {
	item := myList.record[key]
	if item == nil || item.Len() <= 0 {
		return false, index
	}

	length := item.Len()
	if index < 0 {
		index += length
	}

	return index >= 0 && index < length, index
}

func (myList *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}

	if end < 0 {
		end += length
	}

	if start < 0 {
		start = 0
	}

	if end >= length {
		end = length - 1
	}

	return start, end
}
