package myredis

import (
	"fmt"
	"math/rand"

	"github.com/spaolacci/murmur3"
)

const (
	dict_can_resize         = true // 字典是否启用rehash标记
	dict_force_resize_ratio = 5    // 轻质rehash的比率
)

const (
	DICT_HT_SIZE = 4 // 初始化时默认的ht[0].table的大小

)

func hashFuction(data interface{}) uint64 {
	_data := []byte(fmt.Sprintf("%v", data))
	return murmur3.Sum64(_data)
}

// func keyDup(key interface{}) interface{} {

// }

// func valDup(val interface{}) interface{} {

// }

// func keyCampare(key1, key2 interface{}) bool {

// }

// func keyDestrutor(key interface{}) {

// }

// func valDestrutor(val interface{}) {

// }

// 哈希表 在redis中应用很多
// 1 作为数据库底层操作
// 2 哈希键 当哈希键的底层过长时就使用哈希表实现
type DictHt struct {
	// 哈希表数组
	table []*DictEntry
	// 哈希表大小
	size uint
	// 哈希表大小掩码，用于计算索引值 总是等于size-1
	sizemask uint
	// 哈希表中已有节点的数量
	used uint
}

// 重新设置ht的值
func (ht *DictHt) reset() {
	ht.table = nil
	ht.size = 0
	ht.sizemask = 0
	ht.used = 0
}

// resetTable 重新对table进行赋值
func (ht *DictHt) resetTable(size uint) {
	if size == 0 {
		return
	}
	ht.table = make([]*DictEntry, size)
	ht.size = size
	ht.sizemask = size - 1
	ht.used = 0
}

func (ht *DictHt) release() {
	var i uint
	for i < ht.size {
		entry := ht.table[i]
		if entry != nil {
			entry.release()
		}
		entry = nil
		ht.table[i] = nil
		ht.used--
	}
	ht.reset()
}

type DictEntry struct {
	key  interface{} // 值
	val  interface{}
	next *DictEntry // 指向下一个哈希节点 用来解决hash之后index相同的键冲突问题
}

func (entry *DictEntry) release() {
	entry.key = nil
	entry.val = nil
	entry.next = nil
}

func newEntry(key, val interface{}, next *DictEntry) *DictEntry {
	return &DictEntry{
		key:  key,
		val:  val,
		next: next,
	}
}

type Dict struct {
	// 特定的函数类型
	dictType *DictType
	// 哈希表用于存储键值对
	// ht[0] 和ht[1]
	// 当ht[0] 元素过大就会像ht[1] 渐进式哈希
	// ht[1] 在rehash时使用
	ht [2]DictHt
	// 重新散列的索引 哈希表的扩张和收缩工作由此值完成
	// 当不在进行时时，值为-1
	// 记录了目前rehash的进度
	rehashidx int
}

// 哈希表>-1则正在rehash
func (d *Dict) dictRehashing() bool {
	return d.rehashidx > -1
}

// 执行n步渐进式rehash
// false 仍然有键值对需要从ht[0] 迁移到ht[1]
// true 表示已经迁移完毕
func (d *Dict) dictRehash(n int) (bool, error) {
	if !d.dictRehashing() {
		return true, nil
	}
	// 执行n步渐进式hash
	for n >= 0 {
		// 当ht[0]的已有节点数量为0,表示已经迁移完毕
		// 此时需要释放ht[0]中的table
		// 并把ht[0] == ht[1]
		// 并且重新设置ht[1]中的值
		// 然后把rehashidx设置为-1
		if d.ht[0].used == 0 {
			tempHt := d.ht[0]
			// 释放ht[0]中的哈希表
			tempHt.reset()
			// 将ht[1]设置为ht[0]
			d.ht[0] = d.ht[1]
			// 把ht[1]的值进行重新设置
			d.ht[1] = tempHt
			// 关闭rehash
			d.rehashidx = -1
			// 返回true代表已经rehash完毕
			return true, nil
		}

		if d.ht[0].size > uint(d.rehashidx) {
			// rehash 发生越界
			return false, &Error{
				Code: REHASH_OUT_RANGE,
				MSG:  "rehash out range ht[0]",
			}
		}

		// 略过ht[0]数组为空的索引直接指向下一个索引
		for d.ht[0].table[d.rehashidx] == nil {
			d.rehashidx++
		}

		// 获取rehashidx指向的键值对
		// 把dictEntry键值对链表迁移到ht[1]中
		dictEntry := d.ht[0].table[d.rehashidx]
		for dictEntry != nil {
			// 计算在ht[1]中的新的index
			nextEntry := dictEntry.next
			index := d.dictType.hashFuction(dictEntry.key) & uint64(d.ht[1].sizemask)
			// 插入节点到新的hash表
			// 为了减少时间复杂度，这里插入到头部
			dictEntry.next = d.ht[1].table[index]
			d.ht[1].table[index] = dictEntry

			// 更新ht[0]和ht[1]中节点个数
			d.ht[1].used++
			d.ht[0].used--
			// 处理链表中的下一个节点
			dictEntry = nextEntry
		}
		// 把迁移完的hash表的ht[0]的指针设置为空
		d.ht[0].table[d.rehashidx] = nil
		d.rehashidx++
		n--
	}
	return false, nil
}

// 根据key查询键值对
func (d *Dict) dictFind(key interface{}) *DictEntry {
	// 无键值对
	if d.ht[0].size == 0 {
		return nil
	}
	if d.dictRehashing() {
		// 正在rehash, 单步迁移
		d.dictRehash(1)
	}
	hash := d.dictType.hashFuction(key)
	return d.getEntryWithKey(key, hash)
}

// 获取对应的键值对
func (d *Dict) getEntryWithKey(key interface{}, hash uint64) *DictEntry {
	// 由于是渐进式rehash，所以查找key时，需要在ht[0]和ht[1]中查找

	// 查询对应键值对的函数
	findKeyFunc := func(entry *DictEntry) *DictEntry {
		for entry != nil {
			if d.dictType.keyCampare(key, entry.key) {
				return entry
			}
			entry = entry.next
		}
		return nil
	}

	// 在ht[0]中查找
	index0 := hash & uint64(d.ht[0].sizemask)
	dictEntry := findKeyFunc(d.ht[0].table[index0])
	if dictEntry != nil {
		return dictEntry
	}

	if !d.dictRehashing() {
		// 证明没有rehash,只在ht[0]中查找即可
		return nil
	}

	// 在ht[1]中查找
	index1 := hash & uint64(d.ht[1].sizemask)
	dictEntry = findKeyFunc(d.ht[1].table[index1])
	if dictEntry != nil {
		return dictEntry
	}
	return nil
}

func (d *Dict) dictAdd(htIndex int, hash uint64, key, value interface{}) {
	index := hash & uint64(d.ht[htIndex].sizemask)
	entry := newEntry(key, value, d.ht[htIndex].table[index])
	d.ht[htIndex].table[index] = entry
	d.ht[htIndex].used++
}

// 字典需要扩展
func (d *Dict) dictExpend() {
	if d.dictRehashing() {
		// 如果在rehash中，不能进行扩展
		return
	}

	// 如果ht[0].size ==0, 创建并返回一个给定大小的ht[0].table
	if d.ht[0].size == 0 {
		d.ht[0].resetTable(DICT_HT_SIZE)
		return
	}

	// 以下两个条件之一为真时就对字典进行扩展
	// 哈希表的负载因子接近1，也就是已使用的节点数和哈希表大小之间的比率接近1:1
	// 并使dict_can_restsize为真
	// 已使用节点数和字典大小之间的比率超过 dict_force_resize_ratio
	// 如果对哈希表进行扩展扩展的大小为第一个>=ht[0].used*2的2^n(2的n次幂)
	if (d.ht[0].used >= d.ht[0].size && dict_can_resize) || (d.ht[0].used/d.ht[0].size > dict_force_resize_ratio) {
		size := d.getTableSize(2 * d.ht[0].used)
		d.ht[1].resetTable(size)
		d.rehashidx = 0
	}
}

func (d *Dict) release() {
	if d.dictRehashing() {
		// 如果在rehash中，不能进行扩展
		return
	}
	d.ht[0].release()
	d.ht[1].release()
}

// dictReduce 哈希表缩小
func (d *Dict) dictReduce() {
	if d.dictRehashing() {
		// 如果在rehash中，不进行缩小
		return
	}

	// 如果ht[0].size ==0, 不进行缩小
	if d.ht[0].size == 0 {
		return
	}

	// 缩小给定字典
	// 让他的节点数和字典的大小 used / size 接近1:1
	size := d.ht[0].used
	if size < DICT_HT_SIZE {
		// 给定的一个默认最小值
		size = DICT_HT_SIZE
	}
	// 返回一个更小值
	size = d.getTableSize(size)
	d.ht[1].resetTable(size)
	d.rehashidx = 0
}

func (d *Dict) getTableSize(size uint) uint {
	var length uint = 1
	for length >= size {
		length = length << 1
	}
	return length
}

func (d *Dict) dictGetRandomKey() *DictEntry {
	if d.dictRehashing() {
		// 哈希表正则rehash, 进行单步移动
		d.dictRehash(1)
	}

	var dictEntry *DictEntry
	// 在ht[0]或者ht[1]中随机获取一个key
	// 不在rehash时,  d.ht[1].size 为0, 所以如下可以满足两种情况
	for dictEntry == nil {
		index := uint(rand.Intn(int(d.ht[0].size + d.ht[1].size)))
		if index <= d.ht[0].size {
			dictEntry = d.ht[0].table[index]
		} else {
			dictEntry = d.ht[1].table[index-d.ht[0].size]
		}
	}

	// dictEntry 已经指向一个非空的节点链表
	// 从这个链表中随机返回键值对
	entry := dictEntry
	// 计算节点数目
	entryNum := 0
	for dictEntry != nil {
		entryNum++
		dictEntry = dictEntry.next
	}
	// 获取在链表中的随机节点
	randomEntryNum := rand.Intn(entryNum)
	for randomEntryNum > 0 {
		entry = entry.next
	}
	return entry
}

func (d *Dict) dictDelete(key interface{}) error {
	if d.ht[0].size == 0 {
		return &Error{Code: OBJ_PTR_NIL, MSG: "hash table not have value"}
	}

	if d.dictRehashing() {
		// 正在rehash, 单步迁移
		d.dictRehash(1)
	}
	hash := d.dictType.hashFuction(key)
	// 由于是渐进式rehash，所以查找key时，需要在ht[0]和ht[1]中查找

	// 查询对应键值对的函数
	deleteKeyFunc := func(tableIndex int) bool {
		index := hash & uint64(d.ht[tableIndex].sizemask)
		dictEntry := d.ht[tableIndex].table[index]
		var prevDictEntry *DictEntry
		for dictEntry != nil {
			// 查找到对应的key
			// 不是表头节点直接把上一个节点指向节点的下一个节点
			// 是表头节点直接在table的index上存储对应的下一个节点
			// 并且节点数目-1
			if d.dictType.keyCampare(key, dictEntry.key) {
				if prevDictEntry != nil {
					// 指向下一个节点
					prevDictEntry.next = dictEntry.next
				} else {
					// 删除头部节点
					d.ht[tableIndex].table[index] = dictEntry.next
				}
				// 节点数目-1
				d.ht[tableIndex].used--
				// 释放dictEntry指向的值
				dictEntry.release()
				dictEntry = nil
				return true
			}
			prevDictEntry = dictEntry
			dictEntry = dictEntry.next
		}
		return false
	}

	// 在ht[0]中查找
	// 查找到直接返回
	if deleteKeyFunc(0) {
		return nil
	}

	if !d.dictRehashing() {
		// 证明没有rehash,只在ht[0]中查找即可
		return nil
	}

	// 查找到直接返回
	if deleteKeyFunc(1) {
		return nil
	}

	return &Error{Code: KEY_NNOT_FOUND, MSG: "key not found in hash"}
}

// DictCreate 创建一个哈希表
func DictCreate(dictType *DictType) *Dict {
	// 给ht赋初值
	dictht := DictHt{}
	ht := [2]DictHt{dictht, dictht}
	return &Dict{
		dictType:  dictType,
		ht:        ht,
		rehashidx: -1, // 初始值设置为-1
	}
}

func DictAdd(dict *Dict, key, value interface{}) error {
	if dict == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "dict ptr nil"}
	}

	// 执行扩展函数 检测是否对哈希表进行扩展
	dict.dictExpend()

	// 正在rehash, 单步迁移
	if dict.dictRehashing() {
		dict.dictRehash(1)
	}

	hash := dict.dictType.hashFuction(key)
	// 键已经存在
	if dict.getEntryWithKey(key, hash) != nil {
		return &Error{Code: KEY_EXIST, MSG: "key exitis"}
	}
	// 键不存在去添加键
	// 正在rehash把键值对添加到ht[1]的table的头部
	if dict.dictRehashing() {
		dict.dictAdd(1, hash, key, value)
		return nil
	}
	//把键值对添加到ht[0]的table的头部
	dict.dictAdd(0, hash, key, value)
	return nil
}

// DictReplace 将给定的键值添加到dict中, 如果key已经存在则使用新值替换原来的值
func DictReplace(dict *Dict, key, value interface{}) error {
	if dict == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "dict ptr nil"}
	}
	hash := dict.dictType.hashFuction(key)
	dictEntry := dict.getEntryWithKey(key, hash)
	if dictEntry == nil {
		// 键没有存在直接添加
		// 这里hash不可避免计算两次
		return DictAdd(dict, key, value)
	}
	// 使用新值替换旧值
	dictEntry.val = value
	return nil
}

// DictFetchValue 给定给定的key查询value
func DictFetchValue(dict *Dict, key interface{}) *DictEntry {
	if dict == nil {
		return nil
	}
	return dict.dictFind(key)
}

// DictGetRandomKey 从字典中随机返回一个键值对
func DictGetRandomKey(dict *Dict) *DictEntry {
	if dict.ht[0].size == 0 {
		// 字典为空
		return nil
	}
	return dict.dictGetRandomKey()
}

// DictDelete 根据键删除键值对
func DictDelete(dict *Dict, key interface{}) error {
	if dict == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "dict ptr nil"}
	}
	return dict.dictDelete(key)
}

// DictRelease 释放字典以及字典中包含的所有对
func DictRelease(dict *Dict) {
	if dict == nil {
		return
	}
	dict.release()
}

// 特殊的函数类型
type DictType struct {
	// 计算哈希值的函数
	hashFuction func(key interface{}) uint64
	// 复制键的函数
	keyDup func(key interface{}) interface{}
	// 复制值的函数
	valDup func(val interface{}) interface{}
	// 对比键的函数
	keyCampare func(key1, key2 interface{}) bool
	// 销毁键的函数
	keyDestrutor func(key interface{})
	// 销毁值的函数
	valDestrutor func(val interface{})
}
