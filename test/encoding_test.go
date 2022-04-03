package test

import (
	"encoding/json"
	"fmt"
	lredis "learn/l_redis"
	"learn/l_redis/api_go"
	"log"
	"os/exec"
	"testing"
	"time"

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
	maxClient   = 9999  // 最大的连接数目
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
		if data != nil {
			pipe.Set(data.Key, data.Value, 0)
		}
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
// used_memory_rss:240812032  从redis系统角度所占物理内存
// used_memory_rss_human:229.66M 从redis系统角度所占物理内存MB展示
// mem_fragmentation_ratio:1.06 used_memory_rss/used_memory, 碎片化率
// 相比把2倍value写入, 对key进行append会造成大量内存浪（因为SSD预分配机制）
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

//cas4 对string优化，如对于json格式存储的，可以采用ptoto或者采用hash表存储。
// 这里需要根据如下：
// hash-max-ziplist-entries 512
// hash-max-ziplist-value 64
// 做出调整，hash底层有ziplist和hashtable组成
// 当满足上述两个条件为ziplist，不满足为hashtable
// ziplist由于内存连续，并且key-value作为entry分配在一个地址内，占用内存比hashtable小多。
// 不过hashtable的存取时间复杂度为O(n2)，这里需要时间复杂度和空间复杂度做出取舍
func getVideoData(id int64) *api_go.Video {
	return &api_go.Video{
		Id:       id,
		Title:    "迪迦奥特曼",
		VideoURL: "https://image.baidu.com/search/index?ct=201326592&z=undefined&tn=baiduimage&ipn=d&word=奥特曼",
		Playlist: 6494271,
		Playtime: 468,
	}
}

//case4-1 string格式存储json数据，数据为1048575条，key为20字节，value为json数据
// 执行时间为6.357s
// 写入key数量为1048575
/*
memory占用情况为
used_memory:311441504
used_memory_human:297.01M
used_memory_rss:213848064
used_memory_rss_human:203.94M
used_memory_peak:362814480
used_memory_peak_human:346.01M
mem_fragmentation_ratio:0.69
mem_fragmentation_bytes:-97560096

这里操作系统把内存交换到硬盘(swap), 交换60M，由于系统内存不够导致（因为系统内存剩余150M)
*/
func TestWriteUseJsonString(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis compress data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	var key uint32
	client := clients[0]
	var datas []*Data
	for key < maskKey {
		// 计算key
		keyBytes := genNumBites(uint64(key), keyBitLen)
		// 存储json格式数据
		jsonData, err := json.Marshal(getVideoData(int64(key)))
		if err != nil {
			log.Fatalf("json marshal key %d err:%s", key, err.Error())
			return
		}

		data := &Data{
			Key:   bytesToString(keyBytes),
			Value: jsonData,
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
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchWriteData(client, datas); err != nil {
			log.Fatalf("pipe set data err%s", err.Error())
			return
		}
	}

}

type HashData struct {
	Key   string
	Value map[string]interface{}
}

// 把数据写入hash
func batchWriteHash(client redis.Cmdable, datas []*HashData) error {
	pipe := client.Pipeline()
	defer pipe.Close()
	for _, hashData := range datas {
		if hashData != nil {
			pipe.HMSet(hashData.Key, hashData.Value)
		}
	}
	if _, err := pipe.Exec(); err != nil {
		return err
	}
	return nil
}

// 生成video的mapdata，写入hash
func getVideoMapData(id int64) map[string]interface{} {
	return map[string]interface{}{
		"id":       id,
		"title":    "迪迦奥特曼",
		"videoURL": "https://image.baidu.com/search/index?ct=201326592&z=undefined&tn=baiduimage&ipn=d&word=奥特曼",
		"playlist": 6494271,
		"playtime": 468,
	}
}

//case4-2 使用hash存储, hash个数为1048575
// hash-max-ziplist-entries 512 key为20字节，value为对应格式数据
// hash-max-ziplist-value 64
// hash 采用的编码格式为hashtable, 由于videoURL长度为96, ziplist限制长度为64
/* memory占用情况为
used_memory:747651328
used_memory_human:713.02M
used_memory_rss:769003520
used_memory_rss_human:733.38M
used_memory_peak:748442160
used_memory_peak_human:713.77M
mem_fragmentation_ratio:1.07
*/
func TestWriteUseHash(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis write hash data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	var key uint32
	client := clients[0]
	var datas []*HashData
	for key < maskKey {
		// 计算key
		keyBytes := genNumBites(uint64(key), keyBitLen)

		data := &HashData{
			Key:   bytesToString(keyBytes),
			Value: getVideoMapData(int64(key)),
		}

		datas = append(datas, data)
		if len(datas) == dataLimit {
			if err := batchWriteHash(client, datas); err != nil {
				log.Fatalf("pipe set data err%s", err.Error())
				return
			}
			datas = []*HashData{}
		}
		// 对key和value增加1
		key = (key + 1) & maskKey
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchWriteHash(client, datas); err != nil {
			log.Fatalf("pipe set data err%s", err.Error())
			return
		}
	}
}

//case4-3 使用hash存储, hash个数为1048575(再次执行TestWriteUseHash写入)
// hash-max-ziplist-entries 512 key为20字节，value为对应格式数据
// hash-max-ziplist-value 100
// hash 采用的编码格式为ziplist
/* memory占用情况为
used_memory:294668080
used_memory_human:281.02M
used_memory_rss:337014784
used_memory_rss_human:321.40M
used_memory_peak:776056816
used_memory_peak_human:740.11M
mem_fragmentation_ratio:1.14
*/

// 分析使用ziplist编码比hashtable编码节约(713-281）/ 713 = 0.606,节约60%空间
// 相比直接key-value采用json格式存储节约：(297-281)/297=0.055,节约5.5%内存空间
// 但是使用ziplist更新和插入时间复杂度都为O(n*n)

// zlbyte-zltail-zllen-entry1.....-entryN-zlend
// zlbyte uint32 四字节  记录压缩表占用的总内存
// zltail uint32 四字节  记录压缩表尾节点到压缩表起始地址有多少个字节，通过这个偏移量程序无需遍历整个表就可以确定尾
// zllen uint16 2字节   记录压缩表的节点个数，当小于65535时为节点个数，大于时需要遍历整个压缩表才可以得出
// entryx 节点 压缩列表的各个节点，节点长度由保存内容决定
// zlen 1 字节 记录压缩表的尾节点，特殊的值

// entry 存储三种字节数组和六种整数结构
// entry 结构为 previout_entry_length-encoding-content
// previout_entry_length记录上一个entry的长度，当上一个entry程度<254字节，previout_entry_length为1字节
// 当上一个>254字节，previout_entry_length为五字节
// 通过previout_entry_length长度，可以得到上一个entry的地址，因为entry内存地址是连续的。

// 由以上分析ziplist的数据结构特点如下
// 内部表现为数据紧凑的一块连续内存数组
// 可以模拟双向链表结构，O(1)的出队和入队
// 新增删除操作设计内存重新分配或释放，加大了操作的复杂性
// 读写操作设计复杂的指针移动，最坏时间复杂度为O(n*n)
// 适合存储小对象和优先数据

// 在redis中list, zset, hashtable中使用ziplist结构

// hash个数为1048575, key总数为5， value大小不定最长为96字节。
// hash.encoding=hashtable  hash-max-ziplist-value=64
// hash.encoding=ziplist  设置hash-max-ziplist-value=100
// 使用hashtable内存=> used_memory_human:712.43M  总执行时间=>4s(客户端展示执行为11s)   平均执行时间=>usec_per_call=3.95us 712byte/3.95us
// 使用ziplist内存=>used_memory_human:281.02M   总执行时间=>5.9s(程序客户端执行 12.807s) 平均执行时间=>usec_per_call5.626us  281byte/5.626us
// 内存降低比例=>60%   耗时增长比例=> 4倍
// 结论大幅度降低内存占用，操作命令耗时增加

// zset个数为1048575，key总数为5， value大小不定最长为96字节。
// zset.encoding=ziplist  zset-max-ziplist-entries=128, zset-max-ziplist-value=64
// zest.encoding=skiplist zset-max-ziplist-entries=128, zset-max-ziplist-value=150
// zset中添加score=>score全部给定0
// 使用skiplist内存=>1.47G 总执行时间=>命令执行8.2s(程序等待执行17.891s) 执行次数=>1048575 平均执行时间=>7.87us 平均消耗内存=>1.472MB/7.87us
// 使用ziplist内存=>281.02M   总执行时间=>7s(程序执行17.002s) 平均执行时间=>6.71us 执行次数=>1048575 平均消耗内存=>281byte/6.71us
// 内存降低比例=>81.3%   耗时增长比例=> 6倍
// 结论大幅度降低内存占用，操作命令耗时增加

type ZsetData struct {
	Key     string
	Members map[string]interface{}
}

func batchWriteZset(client redis.Cmdable, datas []*ZsetData) error {
	pipe := client.Pipeline()
	defer pipe.Close()
	for _, data := range datas {
		members := make([]redis.Z, len(data.Members))
		index := 0
		for key, value := range data.Members {
			members[index] = redis.Z{
				Score:  0,
				Member: fmt.Sprintf("%s%v", key, value),
			}
			index++
		}
		pipe.ZAdd(data.Key, members...)
	}
	if _, err := pipe.Exec(); err != nil {
		return err
	}
	return nil
}

func TestWriteZset(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis write hash data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}

	var key uint32
	client := clients[0]
	var datas []*ZsetData

	for key < maskKey {
		// 计算key
		keyBytes := genNumBites(uint64(key), keyBitLen)
		data := &ZsetData{
			Key:     bytesToString(keyBytes),
			Members: getVideoMapData(int64(key)),
		}

		datas = append(datas, data)
		if len(datas) == dataLimit {
			if err := batchWriteZset(client, datas); err != nil {
				log.Fatalf("pipe zset data err%s", err.Error())
				return
			}
			datas = []*ZsetData{}
		}
		// 对key和value增加1
		key = (key + 1) & maskKey
	}

	if len(datas) > 0 {
		// 写入没有执行的数据
		if err := batchWriteZset(client, datas); err != nil {
			log.Fatalf("pipe zset data err%s", err.Error())
			return
		}
	}
}

// 对于性能要求较高的场景, 如果使用ziplist。建议长度不超过1000，每个元素大小控制在512byte内。
// 上边的平均耗时使用info commandstas，包含每个命令调用次数、总耗时、平均耗时 单位为us

// 数据写入set, key 20字节，value 16字节, 集合长度1000, 数据量100万条，集合数目1000个
// set.encoding=hashtable set-max-intset-entries=512
// 执行时间比较长是max-clients=10000，在本程序单位时间内达到这个数值，有部分阻塞等待
// 内存=>55.12M 总执行时间=>0.48s(程序执行2.499ss) 执行次数=>1000 平均执行时间=>482.88us 平均消耗内存=> 57797.120byte/482.88us 56.44M/279.45us
// set.encoding=intset    set-max-intset-entries=2000
// 内存=>3.39M 总执行时间=>0.19s(程序执行时间2.912s) 执行次数=>1000 平均执行时间=>194.52us 平均消耗内存=>3557.120byte/194.52us 3.47M/194.52us

// intset相比hashtable内存降低93%， 平均耗时降低60%
// 所以intset相比skiplist测试结果很好
// intset在长度可控情况下，新添加元素时间复杂度为O(n)，查询一个命令时间复杂度为O(long(n))
// 所以如果长度是可控尽量使用intset编码
type SetData struct {
	Key    string
	Values []interface{}
}

func batchWriteSetData(client redis.Cmdable, datas []*SetData) error {
	pipe := client.Pipeline()
	for _, data := range datas {
		pipe.SAdd(data.Key, data.Values...)
	}
	defer pipe.Close()
	if _, err := pipe.Exec(); err != nil {
		return err
	}
	return nil
}

// 执行shell命令
func command(cmd string) error {
	c := exec.Command("bash", "-c", cmd)
	_, err := c.CombinedOutput()
	return err
}

func TestWriteSet(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	command("redis-cli -p 6379 shutdown")
	command("redis-server /usr/local/etc/redis.conf")

	// 休眠1秒钟等待redis重启
	time.Sleep(1 * time.Second)

	clients, err := lredis.Open()
	tag := "redis write set data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}

	var key uint32
	client := clients[0]
	client.FlushAll()         // 清空所有数据
	dataNum := 1000000        // 数据量
	setLen := 1000            // 集合长度
	times := dataNum / setLen // 执行次数
	datas := []*SetData{}

	for key < uint32(times) {
		keyBytes := genNumBites(uint64(key), keyBitLen) // 20 字节

		var value uint16
		valueBitLen := 16                       // 长度10字节 符合uint(16长度)
		maskValue := uint16(1<<valueBitLen - 1) // 用于生成value
		values := make([]interface{}, setLen)
		for value < uint16(setLen) {
			valueBytes := genNumBites(uint64(value), valueBitLen)
			values[value] = valueBytes
			value = (value + 1) & maskValue
		}
		datas = append(datas, &SetData{
			Key:    bytesToString(keyBytes),
			Values: values,
		})

		if len(datas) == maxClient {
			err := batchWriteSetData(client, datas)
			if err != nil {
				t.Fatalf("write set data err: %s", err.Error())
				return
			}
			datas = []*SetData{}
		}
		key = (key + 1) & maskKey
		time.Sleep(1 * time.Microsecond)
	}

	if len(datas) > 0 {
		err := batchWriteSetData(client, datas)
		if err != nil {
			t.Fatalf("write set data err: %s", err.Error())
			return
		}
	}

	// 输出内存和写入set的命令执行情况
	memoryStats, _ := client.Info("memory").Result()
	t.Logf("memory stats=%s\n", memoryStats)
	commandStats, _ := client.Info("commandstats").Result()
	t.Logf("command stats=%s\n", commandStats)
}

// 写入int
func TestWriteSetInInt(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	command("redis-cli -p 6379 shutdown")
	command("redis-server /usr/local/etc/redis.conf")

	// 休眠1秒钟等待redis重启
	time.Sleep(1 * time.Second)

	clients, err := lredis.Open()
	tag := "redis write set data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}

	var key uint32
	client := clients[0]
	client.FlushAll()         // 清空所有数据
	dataNum := 1000000        // 数据量
	setLen := 1000            // 集合长度
	times := dataNum / setLen // 执行次数
	datas := []*SetData{}
	client.ConfigSet("set-max-intset-entries", "2000") // 动态设置整数集合长度为2000

	for key < uint32(times) {
		keyBytes := genNumBites(uint64(key), keyBitLen) // 20 字节

		var value uint16
		valueBitLen := 16                       // 长度10字节 符合uint(16长度)
		maskValue := uint16(1<<valueBitLen - 1) // 用于生成value
		values := make([]interface{}, setLen)
		for value < uint16(setLen) {
			values[value] = value // 这里直接存储int
			value = (value + 1) & maskValue
		}
		datas = append(datas, &SetData{
			Key:    bytesToString(keyBytes),
			Values: values,
		})

		if len(datas) == maxClient {
			err := batchWriteSetData(client, datas)
			if err != nil {
				t.Fatalf("write set data err: %s", err.Error())
				return
			}
			datas = []*SetData{}
		}
		key = (key + 1) & maskKey
		time.Sleep(1 * time.Microsecond)
	}

	if len(datas) > 0 {
		err := batchWriteSetData(client, datas)
		if err != nil {
			t.Fatalf("write set data err: %s", err.Error())
			return
		}
	}

	// 输出内存和写入set的命令执行情况
	memoryStats, _ := client.Info("memory").Result()
	t.Logf("memory stats=%s\n", memoryStats)
	commandStats, _ := client.Info("commandstats").Result()
	t.Logf("command stats=%s\n", commandStats)
}

// redis对五种对象，提供不同编码=>为实现效率和空间的平衡
// redis而且转换规则只能从小内存向大内存转换 主要是编码回退会触发频繁内存开销，所以得不偿失
// 控制编码类型的配置参数如下
// hash  max(sizeof(value)) value所占的最大空间（字节）keyNum hash内key的个数
// if max(sizeof(value)) <= hash-max-ziplist-value && keyNum <=hash-max-ziplist-entries {
// 	   hash.encoding = "zilist"
//    }

// if max(sizeof(value)) > hash-max-ziplist-value || keyNum > hash-max-ziplist-entries {
// 	hash.encoding = "hashtable"
// }

// list 新版本统一使用quicklist, list-max-ziplist-size，表示最大压缩空间或长度
// 最大空间[-5, -1]范围配置，默认-2表示8kb, 正整数表示最大压缩长度，list-compress-depth: 表示最大压缩深度、默认=0表示不压缩

// set if len(set) < set-max-intset-entries && (必须为正整数) => intset 使用整数集合
// if len(set) > set-max-intset-entries || （元素包含一个非整型）=> hashtable

// zset if max(sizeof(value)) > zset-max-ziplist-value || len(zset) > zset-max-ziplist-entries => skiplist 使用跳表
// if max(sizeof(value)) <= zset-max-ziplist-value && len(zset) <= zset-max-ziplist-entries => ziplist 使用压缩表
// max(sizeof(value))为value最大长度，（包含score和member)。len(zset) 为zset内部的元素个数(score, member)
