package myredis

type Server struct {
	Slaves          []*Server    // 自自身具有的从节点
	ID              string       // 每次运行启动生成的id
	SlaveNum        int          // 从客户端的个数
	MaxMemory       int          // 允许设置的最大内存 byte
	MaxMemoryPolicy MemoryPolicy // 允许的内存策略
	DBNum           int          // 数据库的数量
}
