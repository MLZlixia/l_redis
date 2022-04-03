package test

import (
	"fmt"
	lredis "learn/l_redis"
	"log"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"
)

var Debug = false

// redis大量使用key-value结构同样会占用大量内存
// 如果有大量的key-value结构，可以使用memcached。
// 对于存储相同的数据内容-用redis的结构降低外层键的数量，也可以节省大量内存。
// 一般步骤如下
// 通过在客户端预估键规模-客户端程序hash(key)把key映射到对应的hash表

// hash结构降低键数量分析
// 1 如果存在100万个key-value结构，可以映射到1000个hash中，每个hash保存100个元素
// 2 hash的field可用于记录原始的key字符串，方便查找
// 3 hash的value保存原始对象值，确保不超过hash-max-ziplist-value限制
// 根据hash.encoding=ziplist会大幅度降低内存

// 下边测试如下六个keys
// hash-max-ziplist-value=512
// hash-max-ziplist-entries=1000
// maxmemory=2GB
// maxclient=10000
// 数据量100万条
// key 20字节
// hash数量1024个

func getHashDatas(datas []*Data, datasPtr *[]*HashData) {
	maskHash := uint32(len(*datasPtr) - 1)
	for _, data := range datas {
		hashIndex := murmur3.Sum32([]byte(data.Key)) & maskHash
		hashData := (*datasPtr)[hashIndex]
		if hashData == nil {
			hashData = &HashData{
				Key:   fmt.Sprintf("%d", hashIndex),
				Value: map[string]interface{}{},
			}
		}
		hashData.Value[data.Key] = data.Value
		(*datasPtr)[hashIndex] = hashData
	}
}

// 最高位的1
func genNumTopByte(num uint64) uint32 {
	var topBit uint32 = 0
	for num > 0 {
		// 从低位到高位存储
		num = (num - 1) >> 1
		topBit++
	}
	return topBit
}

func getHashData(key uint32, data *Data, datasPtr *[]*HashData) {
	// 此种计算index位置不定
	topBit := genNumTopByte(uint64(len(*datasPtr)))
	maskHash := uint32(1<<topBit - 1)

	hashIndex := maskHash & key
	hashData := (*datasPtr)[hashIndex]
	if hashData == nil {
		hashData = &HashData{
			Key:   fmt.Sprintf("%d", hashIndex),
			Value: map[string]interface{}{},
		}
	}
	hashData.Value[data.Key] = data.Value
	(*datasPtr)[hashIndex] = hashData
}

// writeDataWithCtrlKeyNum 根据键的数目，分别写入hash和string类型
func writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum uint32, writeString bool) {
	command("redis-cli -p 6379 shutdown")
	command("redis-server /usr/local/etc/redis.conf")
	time.Sleep(1 * time.Second)

	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis ctrl key num"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]
	client.FlushAll() // 清空所有数据

	// 设置hash的ziplist的value字节最大
	client.ConfigSet("hash-max-ziplist-value", "512")
	// 设置ziplist的节点个数
	client.ConfigSet("hash-max-ziplist-entries", "1000")

	maxKey := uint32(1<<keyBitLen - 1)
	var dataNum uint32 = 1000000 // 数据量100万条
	var dataLimit uint32 = 10000 // 每次批量写入的条数10000条

	value := make([]byte, valueBitLen)
	datas := []*Data{}
	hashDatas := make([]*HashData, hashNum)
	var key uint32
	for key < dataNum {
		keyBytes := genNumBites(uint64(key), int(keyBitLen))
		data := &Data{
			Key:   bytesToString(keyBytes),
			Value: value,
		}
		getHashData(key, data, &hashDatas)
		datas = append(datas, data)
		if len(datas) == int(dataLimit) {
			var err error
			if writeString {
				err = batchWriteData(client, datas)
			} else {
				err = batchWriteHash(client, hashDatas)
			}

			if err != nil {
				log.Fatalf("write %dbyte err: %s", valueBitLen, err.Error())
				return
			}

			hashDatas = make([]*HashData, hashNum)
			datas = []*Data{}
		}
		key = (key + 1) & maxKey
	}

	if len(datas) > 0 {
		if writeString {
			err = batchWriteData(client, datas)
		} else {
			err = batchWriteHash(client, hashDatas)
		}
	}

	if err != nil {
		log.Fatalf("write %dbyte err: %s", valueBitLen, err.Error())
		return
	}

	// 内存使用情况
	memoryStats, _ := client.Info("memory").Result()
	log.Printf("memory stats=%s\n", memoryStats)
	// key的平均执行时间
	commandStats, _ := client.Info("commandstats").Result()
	log.Printf("command stats=%s\n", commandStats)
	client.FlushAll()
}

// case1 value 512字节
// string类型 占用内存=>589.19M(617809344byte)  执行时间:1117288us(1.11s) 调用次数:1000000 平均耗时:1.12us  平均内存:
// hash-ziplist 占用内存519.77M(545021248byte) 执行时间:37363384us(37s) 调用次数:102400  平均耗时:364.88us 平均内存
// 内存降低比率=>11.88% 平均耗时增加
func TestWrite512ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 512
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite512ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 512
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// case2 value 200字节
// string类型 占用内存: 284.01M   平均耗时: 1.12us     执行次数:1000000
// hash-ziplist 占用内存: 217.43M  平均耗时: 151.75us  执行次数: 102400
// 内存降低比率=>23.6%
func TestWrite200ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 200
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite200ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 200
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// case3 value 100字节
// string类型 占用内存：192.46M    平均耗时: 1.06us     执行次数: 1000000
// hash-ziplist 占用内存: 120.93M    平均耗时: 115.58us    执行次数: 102400
// 内存降低比率=>37.5%
func TestWrite100ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 100
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite100ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 100
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// case3 value 50字节
// string类型 占用内存: 146.68M    平均耗时: 1.07       执行次数：1000000
// hash-ziplist 占用内存: 72.43M    平均耗时: 99.90     执行次数: 102400
// 内存降低比率=>50.6%
func TestWrite50ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 50
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite50ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 50
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// case3 value 20字节
// string类型 占用内存: 116.17M    平均耗时: 1.15us 执行次数: 1000000
// hash-ziplist 占用内存: 43.43M    平均耗时: 98.02    执行次数: 102400
// 内存降低比率：62.9%
func TestWrite20ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 20
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite20ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 20
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// case3 value 5字节
// string类型 占用内存: 100.91M     平均耗时: 1.13us    执行次数: 1000000
// hash-ziplist 占用内存:29.44M    平均耗时: 108.60us   执行次数: 102400
// 内存降低比率:70.2%
func TestWrite5ByteStringValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 5
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, true)
}

func TestWrite5ByteHashValue(t *testing.T) {
	var valueBitLen, hashNum, keyBitLen uint32
	valueBitLen = 5
	hashNum = 1024
	keyBitLen = 20
	writeDataWithCtrlKeyNum(valueBitLen, keyBitLen, hashNum, false)
}

// 对比以上测试数据
// 使用同样的数据ziplist编码hash类型比string类型节约内存
// 节约内存随着value的空间减少越来越明显
// hash-ziplist比string写入耗时，随着value空间减少，时间在减小但是从20byte-5byte(时间升高需要验证)

// 需要根据不同场景对空间复杂度和时间复杂度做出妥协。

// 总结以上对于大量key优化的技巧
// 1 对于小对象存储，使用hash-ziplist优化效率明显, 对于大对象会增加耗时并且优化不明显
// 2 hash-max-ziplist-enries 控制在1000之内，不然存储复杂度在O(n)到O(n*n)之间耗费时间严重
// 3 必须使用hash-ziplist，使用hash-hashtable会增加耗时和内存
// 4 需要预估键的规模，从而确定每个hash需要存储的元素数量，本实例键为100万，为了便于位运算，设置为1024个hash
// 5 需要根据value和key适当调整hash-max-ziplist-enries和hash-max-ziplist-value确保使用ziplist类型编码

// 关于hash的键和field的设计
// 当离散程度较高时，可以按照字符串截取，把后三位作为哈希的field，之前部分自评为哈希的键。
// 当离散程序较低，可以使用一个哈希算法打散键，哈希field存储键的原始值。
// 尽量减少hash键和field的长度，如果可以使用部分键就使用部分键。

// hash-ziplist可以降低内存，但是带来的问题
// 1 需要实时预估键的规模，然后不断调整hash分组，比如不断2倍递增
// 2 使用hash-ziplist编码就无法使用超时和lru淘汰机制自动删除，需要客户端程序维护
// 3 对于大对象，最好不要使用

// 解决以上问题需要开发人员维护一套程序
// 本地程序定时删除(ttl或者lru)、接口访问查询过期自动删除、lru策略删除
