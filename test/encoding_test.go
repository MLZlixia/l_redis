package test

import (
	lredis "learn/l_redis"
	"log"
	"testing"

	"github.com/go-redis/redis"
)

// redis不同编码对空间复杂度和时间复杂度的影响
// 字符串SDS在redis广泛应用-其中预分配策略
// 预分配<1M, 当（appen或者setrange）对于value不足为buf*2+1
// > 1M, 当（appen或者setrange）对于value不足，每次预留=分配1M数据

// 在预分配阶段会造成内存浪费和内存碎片率上升(mem_fragmentation_ratio)

var baseByte byte

const (
	keyBitLen   = 20 // key 占20位长度
	valueBitLen = 60 // value 占60位长度
	maskKey     = uint32(1<<keyBitLen - 1)
	maskValue   = uint64(1<<valueBitLen - 1)
	dataLimit   = 10000 // 批量执行的数据条数
)

func init() {
	baseByte = []byte("0")[0]
}

// 把数字位输出转化为byte
func genNumBites(num uint64, bitLen int) (numBytes []byte) {
	numBytes = make([]byte, bitLen)
	for i := 0; i < bitLen; i++ {
		// 从低位到高位存储
		numBytes[bitLen-i-1] = byte(num & 1)
		num = num >> 1
	}
	return
}

// byte 转换为对应的二进制数字
func bytesToString(dataBytes []byte) string {
	str := ""
	for _, dataByte := range dataBytes {
		str += string(baseByte + dataByte)
	}
	return str
}

// case1 插入maskKey条key-value数据

type Data struct {
	Key   string
	Value []byte
}

// 批量写入二进制数据
func batchWriteData(client redis.Cmdable, datas []*Data) error {
	pipe := client.Pipeline()
	defer pipe.Close()
	for i := 0; i < len(datas); i++ {
		data := datas[i]
		pipe.Set(data.Key, data.Value, 0)
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}
	return nil
}

// 对client插入1048575条数据, 其中key为20字节，value为60字节
// used_memory_human:153.01M 数据所占内存
// used_memory_rss_human:164.74M 从redis系统角度所占的物理内存总量
// mem_fragmentation_ratio:1.08 碎片化率 used_memory_rss/used_memory
func TestSetStringKey(t *testing.T) {
	var key uint32
	var value uint64

	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis compress data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]

	var datas []*Data
	for key < maskKey {
		// 计算key和value的二进制形式
		keyBytes := genNumBites(uint64(key), keyBitLen)
		valueBytes := genNumBites(value, valueBitLen)

		data := &Data{
			Key:   bytesToString(keyBytes),
			Value: valueBytes,
		}
		datas = append(datas, data)

		if len(datas) == dataLimit {
			if err := batchWriteData(client, datas); err != nil {
				log.Fatalf("pipe set data err%s", err.Error())
				return
			}
			datas = []*Data{}
		}
		// 对key和value增加1
		key = (key + 1) & maskKey
		value = (value + 1) & maskValue
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchWriteData(client, datas); err != nil {
			log.Fatalf("pipe set data err%s", err.Error())
			return
		}
	}
}

// case2 对原先1048575条数据执行append，查看内存和碎片率变化，此时key为20字节，value为60字节
// 拼接数据过程阻塞13.399s
// used_memory:361772720        数据占用的内存
// used_memory_human:345.01M    数据占用的内存MB展示  相比于写入2倍数据153M*2=306M, 多分配39M
// used_memory_rss:444833792     从redis系统角度所占物理内存
// used_memory_rss_human:424.23M  从redis系统角度所占物理内存MB展示 rss多分配 424.23M - 164.74 * 2 = 100M
// mem_fragmentation_ratio:1.23   used_memory_rss/used_memory
// 分析以上系统碎片化率上升，因为对key做拼接会预分配空间不用，造成内存浪费，所以避免大量做append操作，可以删除重写

// 在对应key后拼接数据
func batchAppendData(client redis.Cmdable, datas []*Data) error {
	pipe := client.Pipeline()
	defer pipe.Close()
	for i := 0; i < len(datas); i++ {
		data := datas[i]
		value := bytesToString(data.Value)
		pipe.Append(data.Key, value)
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}
	return nil
}

func TestAppendStringKey(t *testing.T) {
	var key uint32
	var value uint64

	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis compress data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]

	var datas []*Data
	for key < maskKey {
		// 计算key和value的二进制形式
		keyBytes := genNumBites(uint64(key), keyBitLen)
		valueBytes := genNumBites(value, valueBitLen)

		data := &Data{
			Key:   bytesToString(keyBytes),
			Value: valueBytes,
		}
		datas = append(datas, data)

		if len(datas) == dataLimit {
			if err := batchAppendData(client, datas); err != nil {
				log.Fatalf("pipe append data err%s", err.Error())
				return
			}
			datas = []*Data{}
		}
		// 对key和value增加1
		key = (key + 1) & maskKey
		value = (value + 1) & maskValue
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchAppendData(client, datas); err != nil {
			log.Fatalf("pipe set data err%s", err.Error())
			return
		}
	}
}

// case3 重新插入1048575条，拼接后的数据，查看内存和碎片率变化, 此时key为20字节，value为120字节
// 批量写入执行时间为5.997s
// used_memory:227555312 数据占用的内存
// used_memory_human:217.01M 数据占用的内存MB展示
// used_memory_rss:240812032
// used_memory_rss_human:229.66M
// mem_fragmentation_ratio:1.06

func TestSetStringKeyAgin(t *testing.T) {
	var key uint32
	var value uint64

	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis compress data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]

	var datas []*Data
	for key < maskKey {
		// 计算key和value的二进制形式
		keyBytes := genNumBites(uint64(key), keyBitLen)
		valueBytes := genNumBites(value, valueBitLen)

		// 把数据扩增到2倍为120字节
		newBytes := make([]byte, len(valueBytes)*2)
		for index := 0; index < len(valueBytes); index++ {
			dataByte := valueBytes[index]
			newBytes[index*2] = dataByte
			newBytes[index*2+1] = dataByte
		}

		data := &Data{
			Key:   bytesToString(keyBytes),
			Value: newBytes,
		}

		datas = append(datas, data)
		if len(datas) == dataLimit {
			if err := batchWriteData(client, datas); err != nil {
				log.Fatalf("pipe set data err%s", err.Error())
				return
			}
			datas = []*Data{}
		}
		// 对key和value增加1
		key = (key + 1) & maskKey
		value = (value + 1) & maskValue
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchWriteData(client, datas); err != nil {
			log.Fatalf("pipe set data err%s", err.Error())
			return
		}
	}
}
