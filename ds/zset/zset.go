package zset

//有序集合sorted set实现

const (
	maxLevel    = 32
	probability = 0.25
)

type (
	//SortedSet sorted set struct
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	//SortedSetNode node of sorted set
	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}

	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	sklNode struct {
		member   string
		score    float64
		backward *sklNode
		level    []*sklLevel
	}

	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int64
	}
)

//New new a sorted set
func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}
