package myredis

const (
	REHASH_OUT_RANGE  = 1  // rehash 越界
	OBJ_PTR_NIL       = 2  //指针为nil
	KEY_EXIST         = 3  // 键已经存在
	KEY_NNOT_FOUND    = 4  // 没有发现对应的键值对
	SKIPNODE_EXIST    = 5  // 跳表的节点已经存在
	MEMBER_NNOT_FOUND = 6  // 对应的member不存在
	OUT_RANGE         = 7  // 超出边界，不在range范围内
	NOT_ALLOW_TYPE    = 8  // 不允许的类型
	VALUE_EXIST       = 9  // 元素已经存在
	VALUE_NOT_FOUND   = 10 // 元素不存在
)

type Error struct {
	Code int
	MSG  string
}

func (e *Error) Error() string {
	return e.MSG
}
