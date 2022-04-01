package test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	lredis "learn/l_redis"
	"learn/l_redis/api_go"
	"log"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"google.golang.org/protobuf/proto"
)

// redis 主从同步大量数据
// 这里127.0.0.1 主从 配置client-output-buffer-limit replication 1kb 1kb 10
// 也就是10秒钟内持续大于1kb或者直接超过1kb，就会redis的复制积压缓冲区溢出
// redis从刚启动->全部复制
// 复制完毕，主写入，redis从根据从offset和主offset差值部分复制
// 在AOF积压缓冲区查看、如果在积压缓冲区部分，不在全量
func BenchmarkReplSyncData(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("open redis err: %s", err.Error())
		return
	}
	client := clients[0]
	zsetName := "repl_test"
	values := make([]redis.Z, 1000000)
	for i := 0; i < 1000000; i++ {
		score := time.Now().UnixNano()
		member := fmt.Sprintf("%d--%d--", i, score)
		value := redis.Z{
			Score:  float64(score),
			Member: member,
		}
		values[i] = value
	}
	_, err = client.ZAdd(zsetName, values...).Result()
	if err != nil {
		log.Printf("set add %s err: %s", zsetName, err.Error())
	}
}

// case1: 当maxmemory 不能满足要求此时报错 OOM command not allowed when used memory > 'maxmemory'.
// 需要在可能情况下动态扩容
// 如果本机内存不足、只能通过“哨兵”+“集群”支持动态扩容

func BenchmarkPipeReplSyncData(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("open redis err: %s", err.Error())
		return
	}
	client := clients[0]
	zsetName := fmt.Sprintf("repl_test_%d", time.Now().UnixNano())
	n := b.N
	values := make([]redis.Z, n)
	for i := 0; i < n; i++ {
		score := time.Now().UnixNano()
		member := fmt.Sprintf("%d--%d--", i, score)
		value := redis.Z{
			Score:  float64(score),
			Member: member,
		}
		values[i] = value
	}
	pipe := client.Pipeline()
	defer pipe.Close()
	pipe.ZAdd(zsetName, values...)
	res, err := pipe.Exec()
	if err != nil {
		log.Printf("set add %s err: %s", zsetName, err.Error())
	}

	for _, cmdRes := range res {
		var val string
		cmd, ok := cmdRes.(*redis.StringCmd)
		if !ok {
			continue
		}
		val, err = cmd.Result()
		if err != nil {
			log.Printf("zset %s fail err: %s", zsetName, err.Error())
		}
		log.Printf("zset %s val:%s success", zsetName, val)
	}
}

type Persion struct {
	Name        string  `protobuf:"bytes,1,opt,name=name,proto3" json:"name"`
	Age         int     `protobuf:"varint,2,opt,name=age,proto3" json:"age"`
	Salary      float64 `protobuf:"fixed32,3,opt,name=salary,proto3" json:"salary"`
	Addr        string  `protobuf:"bytes,4,opt,name=addr,proto3" json:"addr"`
	Description string  `protobuf:"bytes,5,opt,name=description,proto3" json:"description"`
}

// 测试给redis设置过期时间然后删除
// case2: 测试maxmory在配置过期时间时，写入数据的会如何

// 因为redis对过期数据会惰性删除和定时删除，定时删除默认10s运行一次
// 当时删除逻辑根据键的过期比例->使用快慢两种速率模式进行回收
// 默认慢模式->每个数据空间随机检查20个键->-过期键比例<25%->退出
// 默认慢模式->每个数据空间随机检查20个键->-过期键比例>25%->循环执行->执行时间超过25ms->转换到快模式运行
// 所谓快模式就是超时时间设置为1ms，并且在两秒时间内只能运行1次快模式

// 打印超过2022/03/31 20:53:06 exec exprePip  err: OOM command not allowed when used memory > 'maxmemory'.
// 设置为 2 * time.Minute也就是2分钟之后过期
// ======================================
// 在20:55:06 运行查看
// 写入部分数据->过期时间已经生效
// 随着写又洗护发 maxmemory报警

func writeRedisAndPipeExpriation(tag string, n int) {
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]
	pipe := client.Pipeline()
	for i := 0; i < n; i++ {
		score := time.Now().UnixNano()
		member := fmt.Sprintf("%d--%d--", i, score)
		persion := &Persion{
			Name:        member,
			Age:         int(score),
			Salary:      float64(score),
			Addr:        "仙女座星系团银河系-银河系-太阳系-地球-中国-北京-回龙观",
			Description: "人生不相见，动如参与商⑵。\n今夕复何夕，共此灯烛光。",
		}
		persionData, err := json.Marshal(persion)
		if err != nil {
			log.Printf("%s json encoding data %s\n", tag, persionData)
		}

		expriation := (1 + time.Duration(rand.Intn(100))) * time.Minute
		key := fmt.Sprintf("repl_test_%d_%d", rand.Intn(int(score)), i)
		pipe.SetNX(key, string(persionData), expriation)
	}

	defer pipe.Close()
	_, err = pipe.Exec()
	if err != nil {
		log.Printf("exec %s err: %s\n", tag, err.Error())
		return
	}

	// for _, cmdRes := range res {
	// 	cmd, ok := cmdRes.(*redis.BoolCmd)
	// 	if !ok {
	// 		continue
	// 	}
	// 	_, err = cmd.Result()
	// 	if err != nil {
	// 		log.Printf("%s fail err: %s\n", tag, err.Error())
	// 	} else {
	// 		log.Printf("%s success\n", tag)
	// 	}
	// }
}

func BenchmarkExprePipeReplSyncData(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	n := b.N
	writeRedisAndPipeExpriation("expre_pipe_sync_data", n)
}

// case3: 配置redis过期时间+内存管理策略

// redis内存管理策略：1 noeviction 默认策略=> 不会删除任何数据=> 触发 OOM command not allowed when used memory > 'maxmemory'
// noeviction 参见测试方法BenchmarkReplSyncData

// 2 volatitle-lru  根据lru算法设置超时expire,一直到设置为足够位置，没有可删除的键->回退到noeviction策略
// 查看info memory  use-momory 根据过期时间不断变化，如果不设置过期时间，一直报错不能写入
func BenchmarkMemoryPolicyVolatitleLru(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start volatitle lru success")
	n := b.N
	writeRedisAndPipeExpriation("v_lru", n)
	log.Println("end volatitle lru success")
}

// 使用lru但是不设置过期时间 策略为VolatitleLru
// 达到maxmemory=>OOM command not allowed when used memory > 'maxmemory'，退化为noeviction
// 查看 info memory =>used_memory:103728336 used_memory_human:98.92M 已经不能写入
// 短时间内继续访问max number of clients reached=> 连接池超过了最大的允许的数目，我们设置允许的最大client为100，调整这个数值即可。
// 之后释放
func BenchmarkMemoryPolicyVolatitleLruNotExpriation(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start volatitle lru success")
	n := b.N
	writeRedisAndPipeNotExpriation("v_lru_n_exp", n)
	log.Println("end not expriation success")
}

func writeRedisAndPipeNotExpriation(tag string, n int) {
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]
	pipe := client.Pipeline()
	for i := 0; i < n; i++ {
		score := time.Now().UnixNano()
		member := fmt.Sprintf("%d--%d--", i, score)
		persion := &Persion{
			Name:        member,
			Age:         int(score),
			Salary:      float64(score),
			Addr:        "仙女座星系团银河系-银河系-太阳系-地球-中国-北京-回龙观",
			Description: "人生不相见，动如参与商⑵。\n今夕复何夕，共此灯烛光。",
		}
		persionData, err := json.Marshal(persion)
		if err != nil {
			log.Printf("%s json encoding data %s\n", tag, persionData)
		}
		key := fmt.Sprintf("repl_test_%d_%d", rand.Intn(int(score)), i)
		pipe.SetNX(key, string(persionData), 0)
	}

	defer pipe.Close()
	_, err = pipe.Exec()
	if err != nil {
		log.Printf("exec %s err: %s\n", tag, err.Error())
		return
	}

	// for _, cmdRes := range res {
	// 	cmd, ok := cmdRes.(*redis.BoolCmd)
	// 	if !ok {
	// 		continue
	// 	}
	// 	_, err = cmd.Result()
	// 	if err != nil {
	// 		log.Printf("%s fail err: %s\n", tag, err.Error())
	// 	} else {
	// 		log.Printf("%s success\n", tag)
	// 	}
	// }
}

// 3 allkeys-lru 根据lru算法删除键、不管有没有设置超时属性、直到腾出足够空间位置
// 合并批次执行，执行期间发生阻塞查看info clients=100, 达到maxclients限制，目前调整为默认值10000
// 接近maxmemory频繁触发根据lru算法删除所有的键，不触发>memory
func BenchmarkMemoryPolicyAllkeyssLru(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start all keys lru success")
	n := b.N
	// 没有过期时间
	writeRedisAndPipeNotExpriation("all_k_lru", n)
	// 有过期时间
	writeRedisAndPipeExpriation("all_k_lru", n)
	log.Println("end all keys lru")
}

// 4 allkeys-random 随机删除所有键，直到腾出足够空间为止
// 当高并发时触发>memory 错误
// redis服务器接近maxmemory频繁触发随机删除
// 之后腾出足够空间、可以继续写入数据
func BenchmarkMemoryPolicyAllkeysRandom(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start all keys random")
	n := b.N
	// 没有过期时间
	writeRedisAndPipeNotExpriation("all_k_random", n)
	// 有过期时间
	writeRedisAndPipeExpriation("all_k_random", n)
	log.Println("end all keys random")
}

// 5 volatitle-random 随机删除过期键，直到腾出足够空间为止
// 设置的key有随机过期时间
// 回收效率不如allkeys-random高，user_momory维持在maxmemory附近，频繁触发回收，保证写入数据。
func BenchmarkMemoryPolicyVolatitleRandomHaveExpriation(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start v_random_expr")
	n := b.N
	// 有过期时间
	writeRedisAndPipeExpriation("v_random_expr", n)
	log.Println("end v_random_expr")
}

// 在volatitle-random没有过期时间
// 没有过期时间触发>maxmemory错误，volatitle-random只对有过期时间有效
// allowed when used memory > 'maxmemory'
func BenchmarkMemoryPolicyVolatitleRandomNotExpriation(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start v_random_not_expr")
	n := b.N
	// 没有过期时间
	writeRedisAndPipeNotExpriation("v_random_not_expr", n)
	log.Println("end v_random_not_expr")
}

// 6 volatitle-ttl 根据键对象ttl属性，删除最近过期数据，如果没有，退化为noevivtion策略
// 查看info memory 只对最近将要过期数据发生作用，没有会触发memory > 'maxmemory'错误
func BenchmarkMemoryPolicyVolatitleTTL(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start v_ttl")
	n := b.N
	// 有过期时间
	writeRedisAndPipeExpriation("v_ttl", n)
	log.Println("end v_ttl")
}

// 7 volatile-lfu -> 按照LFU策略删除有过期时间的键
func BenchmarkMemoryPolicyVolatitleLFU(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start v_lfu")
	n := b.N
	// 有过期时间
	writeRedisAndPipeExpriation("v_lfu", n)
	log.Println("end v_lfu")
}

// 8 allkeys-lfu -> 按照LFU策略删除所有的键
func BenchmarkMemoryPolicyAllkeysLFU(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	log.Println("start all_key_lfu")
	n := b.N
	// 没有过期时间
	writeRedisAndPipeNotExpriation("all_key_lfu", n)
	// 有过期时间
	writeRedisAndPipeExpriation("all_key_lfu", n)
	log.Println("end all_key_lfu")
}

// case4 有大对象->对打对象更改、删除、更新 => 对于aof和rab的影响，对于salve影响

// case5 当主和从maxmemry配置不一致--->触发错误硬性

// case6 主和从aof积压缓冲区配置不一致----->触发的错误

// case7 主和从复制积压缓冲区配置不一致------->触发的错误

// case7 主和从hashmax-ziplist--entries 配置不一致---->触发的错误

// case8 主和从repl-backlog-size大小配置不一致------> 触发的错误

// 其他关于影响内存、磁盘、元素个数、缓冲区不一样配置请自行去官网查阅去完成。

// 删除大对象
func BenchmarkDelBigObj(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("open redis err: %s", err.Error())
		return
	}
	client := clients[0]
	_, err = client.Del("repl_test").Result()
	if err != nil {
		log.Fatalf("del big obj redis err: %s", err.Error())
		return
	}
}

// 删除大量对象
func BenchmarkDelManyObj(b *testing.B) {
	lredis.ReadConfig("../config.yaml")
	log.Print("=================================\n")
	clients, err := lredis.Open()
	if err != nil {
		log.Fatalf("open redis err: %s", err.Error())
		return
	}
	client := clients[0]
	keys, _ := client.Keys("*").Result()
	delKeys := []string{}
	for _, key := range keys {
		if strings.HasPrefix(key, "repl_test") {
			delKeys = append(delKeys, key)
		}
	}
	_, err = client.Del(delKeys...).Result()
	if err != nil {
		log.Fatalf("del many obj redis err: %s", err.Error())
		return
	}
}

// 内存管理缩减值对象大小
// 1 完整描述业务场景下-尽量缩减键长度
// 2 值长度如果无特殊需求可以把数据转换为二进制编码（测试原始数据、使用json、测试普通二进制、转换为probuf存储压缩比、使用gzip压缩的json的空间占比)
// 3 从业务上精简对象去除必要的存储数据

func getPersionData(index int) *api_go.Persion {
	score := time.Now().UnixNano()
	name := fmt.Sprintf("%d--%d--", index, score)
	return &api_go.Persion{
		Name:        name,
		Age:         int32(index),
		Salary:      float32(score),
		Addr:        "仙女座星系团银河系-银河系-太阳系-地球-中国-北京-回龙观",
		Description: "人生不相见，动如参与商⑵。\n今夕复何夕，共此灯烛光。",
	}
}

func protoEncode(data proto.Message) ([]byte, error) {
	return proto.Marshal(data)
}

func protoDecode(data []byte, item proto.Message) error {
	return proto.Unmarshal(data, item)
}

func gzipEncode(input []byte) ([]byte, error) {
	// 创建一个新的 byte 输出流
	var buf bytes.Buffer
	// 创建一个新的 gzip 输出流
	gzipWriter, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	// 将 input byte 数组写入到此输出流中
	_, err := gzipWriter.Write(input)
	if err != nil {
		_ = gzipWriter.Close()
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	// 返回压缩后的 bytes 数组
	return buf.Bytes(), nil
}

func gzipDecode(input []byte) ([]byte, error) {
	// 创建一个新的 gzip.Reader
	bytesReader := bytes.NewReader(input)
	gzipReader, err := gzip.NewReader(bytesReader)
	if err != nil {
		return nil, err
	}
	defer func() {
		// defer 中关闭 gzipReader
		_ = gzipReader.Close()
	}()
	buf := new(bytes.Buffer)
	// 从 Reader 中读取出数据
	if _, err := buf.ReadFrom(gzipReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 测试redis使用不同编码的数据存储情况
// 加入4万条数据，list格式，总共占用增加19MB

// 各编码占用redis内存如下：
// 运行memory usage key samples 10000 查看每个符合结构预估的占用内存大小，
// samples 后边为采样个数，memory usage 为key名称
// list名称      占用memory
// rwadata      2460899B=2.35mb
// byte_data    9086784B=8.67mb
// proto_data   1989348B=1.897mb
// json_data    2680894B=2.556mb
// gzip_data    2665454B=2.54mb

// 上述测试和理论结果不相符，改进直接存储二进制格式
// list名称      占用memory
// rwadata      2501285B=2.37mb
// byte_data    2501285B=2.37mb
// proto_data   1989348B=1.897mb
// json_data    2680894B=2.556mb
// gzip_data    2671059B=2.5473mb

// 在golang中存储二进制和原始数据大小相同，这是go语言特性决定，[]byte和string类型占用相同
// json_data相比raw_data增大7.18%
// proto_data相比raw_data减少20.46%
// gzip_data相比raw_data增大6.78%
// gzip_data相比json_data减少0.3%, 采用最佳压缩比0.8%

func TestRedisCompressData(t *testing.T) {
	lredis.ReadConfig("../config.yaml")
	clients, err := lredis.Open()
	tag := "redis compress data"
	if err != nil {
		log.Fatalf("%s open redis err: %s", tag, err.Error())
		return
	}
	client := clients[0]
	log.Printf("clients len %d", len(clients))
	pipe := client.Pipeline()
	dataNumber := 10000
	defer func(p redis.Pipeliner) {
		p.Close()
	}(pipe)

	log.Println("===========redis start set rawdata===========")
	rowValues := make([]interface{}, dataNumber)
	for index := 0; index < dataNumber; index++ {
		rowValues[index] = fmt.Sprintf("%v", getPersionData(index))
	}
	pipe.LPush("rawdata", rowValues...)
	_, err = pipe.Exec()
	if err != nil {
		log.Fatalf("redis set rawdata err: %s", err.Error())
		return
	}
	log.Println("===========redis end set rawdata===========")

	log.Println("===========redis start set byte_data===========")
	byteValues := make([]interface{}, dataNumber)
	for index := 0; index < dataNumber; index++ {
		persion := getPersionData(index)
		byteValues[index] = []byte(fmt.Sprintf("%v", persion))
	}
	pipe.LPush("byte_data", byteValues...)
	_, err = pipe.Exec()
	if err != nil {
		log.Fatalf("redis set byte_data err: %s", err.Error())
		return
	}
	log.Println("===========redis end set byte_data===========")

	log.Println("===========redis start set proto_data===========")
	protoValues := make([]interface{}, dataNumber)
	for index := 0; index < dataNumber; index++ {
		persion := getPersionData(index)
		data, err := protoEncode(persion)
		if err != nil {
			log.Fatalf("proto marshal err: %s", err.Error())
			return
		}
		protoValues[index] = data
	}
	pipe.LPush("proto_data", protoValues...)
	_, err = pipe.Exec()
	if err != nil {
		log.Fatalf("redis set proto_data err: %s", err.Error())
		return
	}
	log.Println("===========redis end set proto_data===========")

	log.Println("===========redis start set json_data===========")
	jsonValues := make([]interface{}, dataNumber)
	for index := 0; index < dataNumber; index++ {
		persion := getPersionData(index)
		data, err := json.Marshal(persion)
		if err != nil {
			log.Fatalf("json marshal err: %s", err.Error())
			return
		}
		jsonValues[index] = data
	}
	pipe.LPush("json_data", jsonValues...)
	_, err = pipe.Exec()
	if err != nil {
		log.Fatalf("redis set json_data err: %s", err.Error())
		return
	}
	log.Println("===========redis end set json_data===========")

	log.Println("===========redis start set gzip_data===========")
	gzipValues := make([]interface{}, dataNumber)
	for index := 0; index < dataNumber; index++ {
		persion := getPersionData(index)
		data, err := json.Marshal(persion)
		if err != nil {
			log.Fatalf("json marshal err: %s", err.Error())
			return
		}

		data, err = gzipEncode(data)
		if err != nil {
			log.Fatalf("gzip encode err: %s", err.Error())
			return
		}
		gzipValues[index] = data
	}
	pipe.LPush("gzip_data", gzipValues...)
	_, err = pipe.Exec()
	if err != nil {
		log.Fatalf("redis set gzip_data err: %s", err.Error())
		return
	}
	log.Println("===========redis end set gzip_data===========")
}

// linux 相关命令

// 问题：
// 向redis写入大量数据、并对redis集群开启aof（2s)
// 分别测试删除大对象、删除大量对象
// 结果：
// 删除成功、aof缩小

// 但是磁盘占用却不缩小

// 1 info persistence 查看redis持久化情况-没有发现异常
// 2 info replication offset集群正常、赋值积压缓冲区正常
// 3 info memory      发现内存碎片化率过高
// 4 查看info stats   rejected_connections 被拒绝的连接数16 （由于maxclients限制）（不确定是否被释放--需要排查）
// 5 查看info stats   sync_partial_err 2 被拒绝的部分重新同步(psync)请求的数量  需要排查
// 6 查看info status  total_error_replies 31 响应异常总次数 需要排查是否被释放
// 7 查看info cliets  连接数为1没有发现异常
// 8 查看client list  没有发现异常

// 查看集群memory占用-内存碎片化率过高 mem_fragmemtation_ration 参数
// 怀疑---删除的大量空间不能得到有效利用
// 处理方式--进行安全重启被拒绝的部分重新同步(psync)请求的数量
// 把主节点转变为从节点、然后重启
