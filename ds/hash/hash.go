package hash

type (
	Record map[string]map[string][]byte

	Hash struct {
		record Record
	}
)

func New() *Hash {
	return &Hash{make(Record)}
}

func (h *Hash) HSet(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	h.record[key][field] = value
	return len(h.record[key])
}

func (h *Hash) HSetNx(key string, field string, value []byte) bool {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	// 如果不存在则赋值value否则不赋值
	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return true
	}

	return false
}

func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}
	return h.record[key][field]
}

func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

func (h *Hash) HDel(key, filed string) bool {
	if !h.exist(key) {
		return false
	}
	if _, exist := h.record[key][filed]; exist {
		delete(h.record[key], filed)
		return true
	}
	return false
}

// HExists 检查filed是否存在于key的哈系表中
func (h *Hash) HExists(key, field string) bool {
	if !h.exist(key) {
		return false
	}
	_, exist := h.record[key][field]
	return exist
}

func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}

	return len(h.record[key])
}

func (h *Hash) HKeys(key string) (value []string) {
	if !h.exist(key) {
		return
	}

	for k := range h.record[key] {
		value = append(value, k)
	}
	return
}

func (h *Hash) HValues(key string) (value [][]byte) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		value = append(value, v)
	}
	return
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
