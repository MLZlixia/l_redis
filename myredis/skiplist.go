package myredis

import (
	"math/rand"
	"reflect"
)

const (
	SKIPLIST_MAXVALUE = 32   // 链表生成节点的level的最大容量
	SKIPLIST_P        = 0.25 // Skiplist P = 1/4 每四个节点抽出一个节点作为上一级节点的索引
)

// redis 使用跳跃表作为zset的底层实现之一,
// 如果一个zset元素较多或者某个元素的成员是比较长的字符串时,
// 此时采用的就是跳表

// SkipListNode 保存跳表节点
type SkipListNode struct {
	// 后退指针 指向当前节点的上一个节点
	// 在从表头遍历到表尾时使用
	Backward *SkipListNode
	// 分值 节点保存的数值，在跳跃表中节点按照各自所保存的值从小到大排列
	// 多个节点保存的score对象可以相同
	// 分值相同的节点是按照对象在字典中的顺序大小来进行排序的
	// 成员对象较小的排在前面，表头方向,
	// 成员对象交小的会排在后面表尾巴方向
	Score float32
	// 节点各自保存的成员对象 member
	// 在同一个跳跃表中各个节点保存的成员对象必须是唯一的
	Obj interface{}
	// level数组可以包含多个元素，每个数组都是指向其他节点的指针
	// 创建一个新的跳跃表节点的时候，会随机生成一个[1, 32]之间的数组，这个数组就是层的高度
	// 层的数量越多，访问其他节点的数量就越快
	Level []SkipListLevel
}

// skipNodeExist 节点已经存在
func (node *SkipListNode) skipNodeExist(score float32, member interface{}) bool {
	return node.Score == score && reflect.DeepEqual(node.Obj, member)
}

func (node *SkipListNode) Copy() *SkipListNode {
	return &SkipListNode{
		Backward: node.Backward,
		Score:    node.Score,
		Obj:      node.Obj,
		Level:    node.Level,
	}
}

// SkipListLevel 跳表level中存储的层级节点
type SkipListLevel struct {
	Forward *SkipListNode // 前进指针 当程序从表头向表尾节点遍历时，会沿着前进指针方向进行
	// 跨度 记录了前进指针所指向节点和当前节点的距离
	// 当查找一个节点时，把经过的目标节点的span相加，
	// 得到的就是目标节点在当前跳表中的排位rank
	Span int
}

// slNodeCreate 创建跳表节点
func slNodeCreate(level int, score float32, member interface{}) *SkipListNode {
	levels := make([]SkipListLevel, level)
	return &SkipListNode{
		Score: score,
		Obj:   member,
		Level: levels,
	}
}

// Free 释放指定的节点
func (slNode *SkipListNode) Free() {
	slNode.Backward = nil
	slNode.Score = 0
	slNode.Obj = nil
	for i := 0; i < len(slNode.Level); i++ {
		slNode.Level[i].Forward = nil
		slNode.Level[i].Span = 0
	}
	slNode.Level = nil
}

// SkipList 保存跳表节点的相关信息
// 此结构的作用就是可以更快的处理跳表中的节点
type SkipList struct {
	Header *SkipListNode // 跳表的头节点
	Tail   *SkipListNode // 跳表的尾节点
	// 记录跳表内层数最大的节点的层数(表头节点的层数计算在内)
	// 节点中使用L1,L2,L3表示节点的各个层,
	// 上述分别代表第一层、第二层以此类推,
	// 每个层带有 前进指针forward 后退指针bwward span 跨度
	Level  int
	Length int // 跳表目前包含的节点的数量
}

func (sl *SkipList) Free() {
	if sl.Tail != nil {
		// 从尾部开始释放 backward 为指向前一个节点
		backward := sl.Tail
		for backward != nil {
			node := backward.Backward
			backward.Free()
			backward = node
			node = nil
		}
	}
	sl.Header = nil
	sl.Level = 0
	sl.Tail = nil
	sl.Length = 0
}

// 内部删除函数，被zslDelete、zslDeleteRangeByScore和zslDeleteByRank等函数调用。
func (sl *SkipList) slDeleteNode(skipNode *SkipListNode, updates []*SkipListNode) {
	// 更新所有和被删除节点x有关的节点指针，解除他们之间的关系
	for i := 0; i < sl.Length; i++ {
		if updates[i].Level[i].Forward == skipNode {
			updates[i].Level[i].Span += skipNode.Level[i].Span - 1
			updates[i].Level[i].Forward = skipNode.Level[i].Forward
		} else {
			updates[i].Level[i].Span--
		}
	}
	// 更新被删除节点的前进指针和后退指针
	if skipNode.Level[0].Forward != nil {
		skipNode.Level[0].Forward.Backward = skipNode.Backward
	} else {
		sl.Tail = skipNode.Backward
	}
	// 更新跳表最大层数
	for sl.Level > 1 && sl.Header.Level[sl.Level-1].Forward == nil {
		sl.Level--
	}
	// 跳表节点计数-1
	sl.Length--
}

// SLCreate 创建一个新的跳跃表
func SLCreate() *SkipList {
	sl := &SkipList{
		Level:  1,                                       // 设置起始层数
		Length: 0,                                       // 设置高度
		Header: slNodeCreate(SKIPLIST_MAXVALUE, 0, nil), // 初始化头节点
		Tail:   nil,                                     // 设置表尾 在go中这里不赋值也可
	}
	return sl
}

// SLFree 释放给定跳跃表，以及跳跃表中的所有节点
// 时间复杂度为O(N)
func SLFree(sl *SkipList) {
	if sl == nil {
		return
	}
	sl.Free()
	sl = nil
}

// 返回一个随机值，用作新跳表的层数
// 返回值在[1, SKIPLIST_MAXVALUE]之间,
// 根据随机算法使用的幂次定律,越大的值生成的几率越小。
func slRandomLevel() int {
	level := 1
	for rand.Float32() < SKIPLIST_P && level < SKIPLIST_MAXVALUE {
		level++
	}
	return level
}

// SLInsert 将包含给定的member和score的新节点添加到跳跃表中
// 平均O(logN)，最坏O(N), N为跳跃表的长度
func SLInsert(sl *SkipList, member interface{}, score float32) (*SkipListNode, error) {
	if sl == nil {
		return nil, &Error{Code: OBJ_PTR_NIL, MSG: "skiplist ptr nil"}
	}
	updateLevelList := make([]*SkipListNode, SKIPLIST_MAXVALUE) // 存储用于更新的level
	rankList := make([]int, SKIPLIST_MAXVALUE)                  // 存储level层级的跨度
	// 在每个层级中查找节点的插入位置
	slNode := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		rankList[i] = 0 // 如果i为最后一层跨度为0
		if i < sl.Level-1 {
			// 如果不为第一层，则i层的起始值为i+1层的rank值，
			// 每一层的rank值不断覆盖，最终rankList[0] +1 就是新创建节点的rank值
			rankList[i] = rankList[i+1]
		}
		// 沿着前进指针遍历跳表
		forward := slNode.Level[i].Forward
		memberEqual := reflect.DeepEqual(forward.Obj, member)
		scoreEqual := forward.Score == score
		// 跳表节点已经存在
		if scoreEqual && memberEqual {
			return nil, &Error{Code: SKIPNODE_EXIST, MSG: "skip node exist"}
		}
		// 比对分值
		// 如果分值相等，成员必须不相等
		isNext := (forward != nil && forward.Score < score) || (scoreEqual && !memberEqual)
		for isNext {
			// 记录沿途跨越了多少节点
			rankList[i] = slNode.Level[i].Span
			// 移动到下一个forward
			slNode = slNode.Level[i].Forward
		}
		// 记录将要和新的节点相连接的节点
		updateLevelList[i] = slNode
	}

	// 获取一个随机值作为新节点的层数
	level := slRandomLevel()
	// 新节点的层数没有被创建，需要初始化节点中没有使用的层,
	// 然后记录到update数组中，将来也指向新节点
	maxLevel := sl.Level
	for ; maxLevel < level; maxLevel++ {
		rankList[maxLevel] = 0
		updateLevelList[maxLevel] = sl.Header.Copy()
		updateLevelList[maxLevel].Level[maxLevel].Span = sl.Length
	}
	sl.Level = maxLevel
	// 创建新的节点
	slNodeCreate(level, score, member)
	// 将前面记录的指针指向新的节点，并做相应的设置 O(1)
	for i := 0; i < level; i++ {
		// 设置新的forward指针
		slNode.Level[i].Forward = updateLevelList[i].Level[i].Forward
		// 将沿途每个forward指针的指向新节点
		updateLevelList[i].Level[i].Forward = slNode
		// 计算新节点跨越的节点数量
		slNode.Level[i].Span = slNode.Level[i].Span - (rankList[0] - rankList[i])
		// 更新节点插入之后，沿途的span值。其中+1为添加的一个新节点
		updateLevelList[i].Level[i].Span = rankList[0] - rankList[i] + 1
	}

	// 未删除的节点span+1, 并且直接从表头节点指向新节点 O(1)
	for i := level; i < sl.Level; i++ {
		updateLevelList[i].Level[i].Span++
	}

	// 设置新节点的后退指针
	slNode.Backward = nil
	if updateLevelList[0] == sl.Header {
		slNode.Backward = updateLevelList[0]
	}

	if slNode.Level[0].Forward != nil {
		slNode.Level[0].Forward.Backward = slNode
	} else {
		// 尾节点
		sl.Tail = slNode
	}
	// 跳表节点数目++
	sl.Length++
	return slNode, nil
}

// SLDelete 删除跳表中给定member和score的节点
// 平均O(logN)，最坏O(N), N为跳跃表的长度
func SLDelete(sl *SkipList, member interface{}, score float32) error {
	updateLevelList := make([]*SkipListNode, SKIPLIST_MAXVALUE)
	// 遍历跳表，查找目标节点，并记录所有沿途节点
	slNode := sl.Header
	for i := sl.Length - 1; i >= 0; i-- {
		// 沿着前进指针遍历跳表
		forward := slNode.Level[i].Forward
		isNext := forward != nil && (forward.Score == score && !reflect.DeepEqual(forward.Obj, member))
		for (forward != nil && forward.Score < score) || isNext {
			// 移动到下一个forward
			slNode = slNode.Level[i].Forward
		}
		// 记录将要和新的节点相连接的节点
		updateLevelList[i] = slNode
	}

	// 只有当score和member都相等时才删除
	slNode = slNode.Level[0].Forward
	if slNode.skipNodeExist(score, member) {
		sl.slDeleteNode(slNode, updateLevelList)
		slNode.Free()
		slNode = nil
		return nil
	}
	return &Error{Code: MEMBER_NNOT_FOUND, MSG: "skip list not have this member"}
}

// SLGetRank 返回给定member和score的节点在跳表中的排位
// 平均O(logN)，最坏O(N), N为跳跃表的长度
// 因为跳表的表头也被计算在内，所以返回的排位以1为起始值
func SLGetRank(sl *SkipList, member interface{}, score float32) (int, error) {
	if sl == nil {
		return 0, &Error{Code: OBJ_PTR_NIL, MSG: "skipList ptr nil"}
	}
	rank := 0
	slNode := sl.Header
	// 遍历跳表
	for i := sl.Level - 1; i >= 0; i-- {
		// 遍历节点并对比元素
		// 沿着前进指针遍历跳表
		forward := slNode.Level[i].Forward
		isNext := forward != nil && (forward.Score == score && !reflect.DeepEqual(forward.Obj, member))
		for (forward != nil && forward.Score < score) || isNext {
			// 移动到下一个forward
			rank += slNode.Level[i].Span
			slNode = forward
		}
		// 查找到对应的值
		if slNode.skipNodeExist(score, member) {
			return rank, nil
		}
	}
	return 0, nil
}

// SLGetElementByRank 返回跳表在给定排位上的节点
// 平均O(logN)，最坏O(N), N为跳跃表的长度
func SLGetElementByRank(sl *SkipList, rank int) (*SkipListNode, error) {
	if sl == nil {
		return nil, &Error{Code: OBJ_PTR_NIL, MSG: "skipList ptr nil"}
	}
	spanSum := 0
	slNode := sl.Header
	// 遍历整个跳表
	for i := sl.Level - 1; i >= 0; i++ {
		// 遍历节点并对比元素
		for slNode.Level[i].Forward != nil && ((spanSum + slNode.Level[i].Span) < rank) {
			spanSum += slNode.Level[i].Span
			slNode = slNode.Level[i].Forward
		}
		// 如果spanSum==rank 证明越过的节点数目已经满足要求
		if spanSum == rank {
			return slNode, nil
		}
	}
	return nil, nil
}

// SLIsInRange 查询分值范围是否在跳表中
// 通过跳跃表的头节点和尾节点，可以在O(1) 时间内完成
func SLIsInRange(sl *SkipList, rangeNum [2]float32) error {
	if sl == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "skipList ptr nil"}
	}

	if len(rangeNum) < 2 {
		return &Error{Code: OUT_RANGE, MSG: "out of range"}
	}
	min := rangeNum[0]
	max := rangeNum[1]
	if min >= max {
		return &Error{Code: OUT_RANGE, MSG: "out of range"}
	}

	// 检查最大值
	maxNode := sl.Tail
	if maxNode == nil || maxNode.Score < min || maxNode.Score < max {
		return &Error{Code: OUT_RANGE, MSG: "out of range"}
	}
	// 检查最小值
	minNode := sl.Header.Level[0].Forward
	if minNode == nil || minNode.Score > max || minNode.Score > min {
		return &Error{Code: OUT_RANGE, MSG: "out of range"}
	}
	return nil
}

// SLFirstInRange 给定一个分值范围，返回第一个符合这个范围的节点
// 平均O(logN)，最坏O(N), N为跳跃表的长度
func SLFirstInRange(sl *SkipList, rangeNum [2]float32) (*SkipListNode, error) {
	if err := SLIsInRange(sl, rangeNum); err != nil {
		return nil, err
	}
	// 遍历跳表，找出符合范围min项的节点
	slNode := sl.Header
	for i := sl.Level - 1; i >= 0; i++ {
		// 当score >= min时，退出循环，则slNode.Level[0].Forward为符合的节点
		for slNode.Level[i].Forward != nil && slNode.Score < rangeNum[0] {
			slNode = slNode.Level[i].Forward
		}
	}
	return slNode.Level[0].Forward, nil
}

// SLLastInRange 给定一个分值范围，返回最后一个符合这个范围的节点
// 平均O(logN)，最坏O(N), N为跳跃表的长度
func SLLastInRange(sl *SkipList, rangeNum [2]float32) (*SkipListNode, error) {
	if err := SLIsInRange(sl, rangeNum); err != nil {
		return nil, err
	}
	// 遍历跳表，找出符合范围min项的节点
	slNode := sl.Header
	for i := sl.Level - 1; i >= 0; i++ {
		// max := rangeNum[1]
		// 当score > max时，退出循环，slNode 为满足条件的最后一个值
		for slNode.Level[i].Forward != nil && slNode.Score < rangeNum[1] {
			slNode = slNode.Level[i].Forward
		}
	}
	return slNode, nil
}

// SLDeleteRangeByScore 给定一个分值范围，删除跳表中所有在这个范围之内的节点
// 节点不仅在跳表内删除，也在相应的字典中删除
// O(N), N为节点的数量
func SLDeleteRangeByScore(sl *SkipList, rangeNum [2]float32, dict *Dict) (int, error) {
	if err := SLIsInRange(sl, rangeNum); err != nil {
		return 0, nil
	}
	slNode := sl.Header
	updates := make([]*SkipListNode, SKIPLIST_MAXVALUE)
	// 记录和被删除节点相关的节点
	for i := sl.Level - 1; i >= 0; i++ {
		forward := slNode.Level[i].Forward
		for forward != nil && forward.Score < rangeNum[0] {
			slNode = forward
		}
		updates[i] = slNode
	}
	removed := 0
	// 定位到给定范围开始的一个节点
	slNode = slNode.Level[0].Forward
	// 删除范围内的所有的节点
	for slNode != nil && slNode.Score <= rangeNum[1] {
		next := slNode.Level[0].Forward
		// 在跳表删除
		sl.slDeleteNode(slNode, updates)
		// 在哈希表删除
		DictDelete(dict, slNode.Obj)
		slNode.Free()
		slNode = next
		removed++
	}
	return removed, nil
}

// SLDeleteRangeByRank 给定一个排位范围，删除跳表中所有符合这个范围的点
func SLDeleteRangeByRank(sl *SkipList, start, end int, dict *Dict) (int, error) {
	if sl == nil {
		return 0, &Error{Code: OBJ_PTR_NIL, MSG: "skipList ptr nil"}
	}
	updates := make([]*SkipListNode, SKIPLIST_MAXVALUE)
	var traversed, removed int
	// 沿着前进指针到指定位置，并记录沿途的所有的指针
	slNode := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for slNode.Level[i].Forward != nil && (traversed+slNode.Level[i].Span < start) {
			traversed += slNode.Level[i].Span
			slNode = slNode.Level[i].Forward
		}
		updates[i] = slNode
	}
	// 移动到起始排位的第一个节点
	slNode = slNode.Level[0].Forward
	traversed++
	// 删除所有在内定范围内的节点
	for slNode != nil && traversed <= end {
		next := slNode.Level[0].Forward
		sl.slDeleteNode(slNode, updates) // 跳表中删除
		DictDelete(dict, slNode.Obj)     // 字典中删除
		slNode.Free()                    // 释放节点
		slNode = next
		removed++   // 删除计数器增加
		traversed++ // 排位计数器+1
	}
	return removed, nil
}
