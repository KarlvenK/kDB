package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

//SkipList是跳表的实现，跳表是一个高效的可替代平衡二叉搜索树的数据结构
//它能够在O(log(n))的时间复杂度下进行插入、删除、查找操作
//跳表的具体解释可参考Wikipedia上的描述：https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8

const (
	//最大索引层数
	maxLevel int = 18

	probability float64 = 1 / math.E
)

//遍历节点的函数， bool返回false时遍历结束
type handleEle func(e *Element) bool

type (
	// Element 跳表存储元素定义
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	//Node 跳表节点
	Node struct {
		next []*Element //next slice的len值代表这个节点的跳表高度
	}

	//SkipList 跳表定义
	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randSource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

// NewSkipList 初始化一个空跳表
func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

//Key 跳表元素 key
func (e *Element) Key() []byte {
	return e.key
}

//Value 跳表元素 value
func (e *Element) Value() interface{} {
	return e.value
}

//SetValue set the elem val
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

//Next 跳表的第一层索引是原始数据，有序排序，可根据next方法获得一个串联所有数据的链表
func (e *Element) Next() *Element {
	return e.next[0]
}

//Front get the head element
//	e := list.Front()
//	for p := e; p!= nil; p = p.next() {
//		-----
//	}
func (t *SkipList) Front() *Element {
	return t.next[0]
}

//Put store a element to skiplist, if the key already exists, update the value
//因此此链表暂时不支持相同的key
func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)

	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 { //这里的小于等于实际上并不可能取到小于
		element.value = value
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, t.randomLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}
	t.Len++
	return element
}

//Get 根据key查找对应的 Element 元素
//未找到返回nil
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}
	return nil
}

//Exist judge if the key exists
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove 根据key 删除跳表中对应的元素，返回删除后元素的指针
func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}
	return nil
}

// Foreach 遍历跳表中的每一个元素
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

//找到key对应的前一个节点索引的信息
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node //头节点（不存值）
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i] //这里i是层数

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

// FindPrefix 找到第一个和前缀匹配的Element
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = t.Front()
	}

	return next
}

//生成所以随机层数
func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randSource.Int63()) / (1 << 63)

	level = 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
