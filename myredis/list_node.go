package myredis

// redis中链表的节点
type ListNode struct {
	prev  *ListNode // 前置节点
	next  *ListNode // 后置节点
	value *SDS      // 存储的值
}

func NewNode(value *SDS) *ListNode {
	return &ListNode{value: value}
}

func (node *ListNode) prevNode() *ListNode {
	return node.prev
}

func (node *ListNode) nextNode() *ListNode {
	return node.next
}

func (node *ListNode) nodeValue() *SDS {
	return node.value
}

// 链表在redis的列表键、发布订阅、慢查询、监视器都有使用
// 头结点的pre指向nil，尾节点的next也指向nil 是一个双端链表
type List struct {
	head  *ListNode                         // 链表的头节点
	tail  *ListNode                         // 链表的尾节点
	len   uint                              // 链表中元素的数量
	dup   func(*ListNode) *ListNode         // 节点复制函数
	free  func(*ListNode)                   // 节点释放函数
	match func(node1, node2 *ListNode) bool // 节点比较函数
}

// ListSetDupMethod 将给定的函数设置为链表的复制函数
func (l *List) ListSetDupMethod(dup func(*ListNode) *ListNode) {
	l.dup = dup
}

// ListGetDupMethod 获取链表的节点复制函数
func (l *List) ListGetDupMethod() func(*ListNode) *ListNode {
	return l.dup
}

// ListSetFreeMethod 将给定的函数设置为链表的节点释放函数
func (l *List) ListSetFreeMethod(free func(*ListNode)) {
	l.free = free
}

// ListGetFreeMethod 获取链表的节点释放函数
func (l *List) ListGetFreeMethod() func(*ListNode) {
	return l.free
}

// ListSetMatchMethod 将给定的函数设置为链表的节点比较函数
func (l *List) ListSetMatchMethod(match func(node1, node2 *ListNode) bool) {
	l.match = match
}

// ListGetMatchMethod 获取链表的节点比较函数
func (l *List) ListGetMatchMethod() func(node1, node2 *ListNode) bool {
	return l.match
}

// ListLehgth 返回链表中节点的个数
func (l *List) ListLehgth() uint {
	return l.len
}

// ListFirst 表头节点
func (l *List) ListFirst() *ListNode {
	return l.head
}

// 返回链表的尾节点
func (l *List) ListLast() *ListNode {
	return l.tail
}

// ListPrevNode 返回指定节点的上一个节点
func (l *List) ListPrevNode(node *ListNode) *ListNode {
	if node == nil {
		return nil
	}
	return node.prevNode()
}

// ListNextNode 返回指定节点的下一个节点
func (l *List) ListNextNode(node *ListNode) *ListNode {
	if node == nil {
		return nil
	}
	return node.nextNode()
}

// ListNodeValue 返回指定节点的存储的值
func (l *List) ListNodeValue(node *ListNode) *SDS {
	if node == nil {
		return nil
	}
	return node.nodeValue()
}

// ListCreate 创建一个不包含任何节点的新的链表
func (l *List) ListCreate() *List {
	return &List{}
}

// 将一个包含给定值的新节点添加到表头
func (l *List) ListAddNodeHead(value *SDS) {
	node := NewNode(value)
	l.listAddNodeHead(node)
}

func (l *List) listAddNodeHead(node *ListNode) {
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		head := l.head
		node.next = head
		head.prev = node
		l.head = node
	}
	l.len++
}

// 将一个包含给定值的新节点添加到表尾
func (l *List) ListAddNodeTail(value *SDS) {
	node := NewNode(value)
	if l.tail == nil {
		l.head = node
		l.tail = node
	} else {
		l.tail.next = node
		node.prev = l.tail
	}
	l.len++
}

// 将一个包含给定值的新节点添加到指定的节点前
func (l *List) ListInsertNode(value *SDS, insertPosition *ListNode) {
	if insertPosition == nil {
		return
	}

	if insertPosition == l.head {
		l.ListAddNodeHead(value)
		return
	}

	node := NewNode(value)
	insertPosition.prev.next = node
	node.prev = insertPosition.prev
	node.next = insertPosition
	insertPosition.prev = node
	l.len++
}

// 查找并返回链表中包含给定值的节点
func (l *List) ListSearchKey(key *SDS) *ListNode {
	node := l.head
	for node != nil {
		if node.value == key {
			return node
		}
		node = node.next
	}
	return nil
}

// 返回链表在给定索引上的节点
func (l *List) ListIndex(index uint) *ListNode {
	node := l.head
	var i uint
	for i < l.len {
		if i == index-1 {
			return node
		}
		node = node.next
	}
	return nil
}

// 从链表中删除给定节点
func (l *List) ListDelNode(node *ListNode) {
	// 删除头节点
	if node == l.head {
		l.ListDelHead()
		return
	}

	// 删除尾节点
	if node == l.tail {
		l.ListDelTail()
		return
	}

	// 删除普通节点
	node.prev = node.next
	node.next.prev = node.prev
	l.len--
}

// ListDelHead 删除头节点
func (l *List) ListDelHead() {
	if l.len <= 0 {
		return
	}
	next := l.head.next
	l.head = next
	if next != nil {
		next.prev = nil
	} else {
		l.tail = nil
	}
	l.len--
}

// ListDelTail 删除尾节点
func (l *List) ListDelTail() {
	if l.len <= 0 {
		return
	}
	prev := l.tail.prev
	if prev != nil {
		prev.next = nil
	} else {
		l.head = nil
	}
	l.len--
}

// 将链表的尾节点弹出，然后将被弹出的节点放入链表的表头，成为新的头结点
func (l *List) ListRotate() {
	// 弹出尾节点
	tail := l.tail
	l.ListDelTail()
	// 把尾节点添加到头成为新的头节点
	l.listAddNodeHead(tail)
}

// 赋值一个给定链表的副本
func (l *List) ListDup() *List {
	return &List{
		head:  l.head,
		tail:  l.tail,
		len:   l.len,
		dup:   l.dup,
		free:  l.free,
		match: l.match,
	}
}

// 释放给定链表，以及链表中的所有节点
func (l *List) ListRelease() {
	node := l.head
	for node != nil {
		node.prev = nil
		node = node.next
	}
	node = nil
	l.head = nil
	l.tail = nil
	l.dup = nil
	l.free = nil
	l.match = nil
	l.len = 0
}
