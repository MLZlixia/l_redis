package myredis

import "errors"

type MemoryPolicy int

const (
	Noeviction = iota + 1 // 从不删除

	VolatileLRU // 根据lru算法删除过期时间
	AllKeysLRU  // 根据lru算法删除所有的

	VolatileRandom // 随机删除有过期时间的
	ALLKeysRandom  // 随机删除所有的

	VolatileLFU // 根据lfu算法删除过期时间
	AllKeysLFU  // 根据lfu算法所有的

	VolatileTTL // 删除最近过期的键、没有退化为noeviction
)

// 已经使用的内存
func usedMemory() int {
	return 0
}

// 主从的时，从的节点缓冲区
func salvesOutputBufferSize(slaves int) int {
	return 256
}

// aof重写的时候缓冲区
func aofRewriteBufferSize() int {
	return 100
}

// redis 对应8中内存回收策略，这里内存回收伪代码如下
func freeMemoryIfNeed(server *Server) (isNeed bool, err error) {
	var memUsed, memToFree, memFreed int
	// 计算当前内存总量、排除从节点缓冲区和AOF缓冲区的作用
	slaves := server.SlaveNum
	memUsed = usedMemory() - salvesOutputBufferSize(slaves) - aofRewriteBufferSize()

	// 当前使用内存未达到使用边界
	if memUsed <= server.MaxMemory {
		return
	}

	if server.MaxMemoryPolicy == Noeviction {
		// 不进行淘汰
		err = errors.New("OOM command not allowed when used memory > 'maxmemory'")
		return
	}

	// 计算需要释放多少内存
	memToFree = memUsed - server.MaxMemory
	// 根据maxmemory-policy策略循环删除 释放内存
	for memFreed < memToFree {
		// 迭代redis所有db空间
	}

	return
}
