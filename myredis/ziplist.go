package myredis

import "unsafe"

// 压缩表是尽可能节约内存的一种数据存储方式
// 压缩表是列表键和哈希键的底层实现之一
// 当一个列表只包含少量列表项，并且每个列表项要么是小整数或者短字符串，那么就是使用压缩表实现列表。
// 当一个哈希键只包含少量键值对，并且每个键值对的键和值要么是小整数或者短字符串，
// redis就会使用压缩表实现哈希键值对。

type Entry struct {
	// 1 字节或者5字节
	// go中这里使用int64 8字节存储前边使用0表示
	// 存储前一个entry的字节数，前一个entry小于254字节，则此属性用1字节存储
	// 当大于254字节，使用5字节存储 属性的第一个字节会被设置为0xFE
	PreviousEntryLength uint64

	// 1 字节、2字节、或者5字节长, 存储了content属性所保存的类型以及长度
	// 值的最高位为00，01，或者10的是字节数组编码，表示存储着字节数组
	// 数组的长度由除去最高位的两位获得

	// 1字节长值的最高为以11开头的是整数编码，表示content属性存储着整数类型的值
	// 整数的类型和长度由除去最高两位之后的其他位获得
	Enconding uint64

	// 存储的数据
	// 小于(2^6-1)字节的字节数组
	// 小于(2^14-1)字节的字节数组
	// 小于(2^32-1)字节的字节数组

	// 4位长，介于1和12之间的无符号整数
	// 1字节长的有符号整数
	// 3 字节长的有符号整数
	// int16类型整数
	// int32类型整数
	// int64类型整数
	Content unsafe.Pointer
}

type ZipList struct {
	// 使用4字节存储存储 记录整个压缩表占用的内存字节数
	// 对压缩表进行内存分配或者计算zlen时使用
	Bytes uint32
	// 4字节 记录压缩表表尾节点距离压缩表的起始地址的字节
	// 通过这个偏移量，程序可以直接确定表尾节点的地址
	Tail uint32
	// 2 字节 记录压缩表包含的节点数量，当len < uint16_max(65535),记录的是真实数量
	// len == uint16_max,需要遍历整个压缩表
	Len uint16
	//  特殊值十进制，用于标记压缩表的末端
	Lend uint8
	// 存储的压缩表的节点
	Entrys []*Entry
}

/*
空白 ziplist 示例图

area        |<---- ziplist header ---->|<-- end -->|

size          4 bytes   4 bytes 2 bytes  1 byte
            +---------+--------+-------+-----------+
component   | zlbytes | zltail | zllen | zlend     |
            |         |        |       |           |
value       |  1011   |  1010  |   0   | 1111 1111 |
            +---------+--------+-------+-----------+
                                       ^
                                       |
                               ZIPLIST_ENTRY_HEAD
                                       &
address                        ZIPLIST_ENTRY_TAIL
                                       &
                               ZIPLIST_ENTRY_END

非空 ziplist 示例图

area        |<---- ziplist header ---->|<----------- entries ------------->|<-end->|

size          4 bytes  4 bytes  2 bytes    ?        ?        ?        ?     1 byte
            +---------+--------+-------+--------+--------+--------+--------+-------+
component   | zlbytes | zltail | zllen | entry1 | entry2 |  ...   | entryN | zlend |
            +---------+--------+-------+--------+--------+--------+--------+-------+
                                       ^                          ^        ^
address                                |                          |        |
                                ZIPLIST_ENTRY_HEAD                |   ZIPLIST_ENTRY_END
                                                                  |
                                                        ZIPLIST_ENTRY_TAIL
*/

const (
	ZL_BYTES = 1 << 32 // 4 字节
	ZL_TAIL  = 1 << 32 // 4字节
	ZL_LEN   = 1 << 16 // 2 字节
	ZL_END   = 1 << 8  // 1字节
)

const (
	// 用于表示zlBytes所占的字节数
	bytesLength = 4
	// 字节和位的转换 1字节=8bit
	bitCarry = 8
)

func saveZltail(zl *[]byte, zlTail uint32) {

}

func saveZllen(zl *[]byte, zllen uint16) {

}

// 上述是对压缩表结构的模型构建，使用[]byte存储压缩表即可
// ZiplistNew 创建一个ziplist T=O(1)
func ZiplistNew() []byte {
	zlSize := (ZL_BYTES + ZL_TAIL + ZL_LEN + ZL_END) / ZL_END
	zl := make([]byte, zlSize)
	// ptr := *(*int64)(unsafe.Pointer(&zl))
	saveZlBytes(zl)
	zl[zlSize-1] = 1
	return zl
}

func saveZlBytes([]byte) {

}

// ZiplistPush 将给定的值推入到ziplist
// 创建一个新的节点根据where参数，如果where==ZIPLIST_HEAD表头，否则表尾巴
// T=O(n^2)
func ZiplistPush() {

}

// ZiplistInsert 将包含指定s值插入到指定的位置p中
// 如果p指向一个节点，那么把新节点放到原有节点的前面
// T=O(n^2)
func ZiplistInsert() {

}

// 返回压缩列表给定索引上的节点 O(N)
// 如果索引为正从表头向表尾遍历，如果索引为负数从表尾巴向表头遍历
// 正数索引从0开始，负数索引从-1开始
// 如果索引超过列表的节点数量，或者列表为空，返回nil
func ZiplistIndex() {

}

// 寻找节点值和vstr相等的列表节点，并返回该节点的指针。
// 每次比对之前都跳过skip节点
// 查询不到响应的节点返回nil
// T=O(n^2)
func ZiplistFind() {

}

// 返回P指向的后置节点
// 如果p为表末端，或者p已经是表尾节点，返回nil
// T=O(1)
func ZiplistNext() {

}

// 返回P指向的前置节点
// 如果p指向空列表，或者p已经是表头，返回nil
// T=O(1)
func ZiplistPrev() {

}

// 取出P指向的节点
// 如果节点保存的是字符串，那么将值保存在*str中。
// 如果节点保存的是整数，那么将节点保存在*num中。
// 如果p为空列表，或者p指向列表末端，返回nil。
// T=O(1)
func ZiplistGet() {

}

// 从zl中删除p所指向的节点,并且从原地更新p所指向的位置，
// 使得可以在迭代列表的过程中对节点进行删除
// T=O(N^2)
func ZiplistDelete() {

}

// 从index指定的索引开始，连续的从zl中删除num个节点。
// T=O(n^2)
func ZiplistDeleteRange() {

}

// 返回整个ziplist所占用的字节数
// T=O(1)
func ZiplistBlobLen() {

}

// zl中节点的个数 T=O(n)
func ZiplistLen() {

}
