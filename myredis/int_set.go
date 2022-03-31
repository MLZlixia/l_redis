package myredis

import (
	"math/rand"
	"unsafe"
)

const (
	SET_MAX_VALUE = 1024
)

const (
	INTSET_ENC_INT8  = 1 << 3
	INTSET_ENC_INT16 = 1 << 4
	INTSET_ENC_INT32 = 1 << 5
	INTSET_ENC_INT64 = 1 << 6
)

// 整数集合
type Intset struct {
	// 编码方式
	// INTSET_ENC_INT8
	// INTSET_ENC_INT16
	// INTSET_ENC_INT32
	// INTSET_ENC_INT64
	// 上述四种值的一种
	Encoding uint
	// 包含的元素数量
	Length int
	// 包含的元素的数量
	// contents 数组的类型取决于encoding的值
	// 当为INTSET_ENC_INT8时Contents为[]int8
	// 当为INTSET_ENC_INT16时Contents为[]int64
	// 当向整数集合中插入一个新值时，sizeOf(value) > Encoding
	// 则contents数值都进行扩增到响应的类型，并且个数保持不变，然后把值存储到对应的位置
	// Contents 为有序表
	// cintents 只升级不降级，比如从INTSET_ENC_INT32变为INTSET_ENC_INT64之后，不会变回去
	contents    []unsafe.Pointer // 保存元素的数组
	contentsLen int
}

func (intset *Intset) saveNumWithEncoding(encoding uint, numptr unsafe.Pointer) error {
	if intset.Encoding < encoding {
		// 内部的类型值 需要扩增 O(n)
		for i := 0; i < intset.Length; i++ {
			newPtr := intset.convertTypeWithEnconding(encoding, intset.contents[i])
			intset.contents[i] = newPtr
		}
	}
	if encoding < intset.Encoding {
		encoding = intset.Encoding
	}
	// 保存值时需要对值进行排序
	ptr := intset.convertTypeWithEnconding(encoding, numptr)
	if ptr == nil {
		return &Error{Code: NOT_ALLOW_TYPE, MSG: "encoding must in [int8, int16, int32, int64]"}
	}

	if intset.contentsLen > intset.Length {
		// 如果有多个未用空间需要直接存入对应的下标即可
		intset.contents[intset.Length-1] = ptr
	} else {
		intset.contents = append(intset.contents, ptr)
		intset.contentsLen++
	}
	intset.Length++ // 长度增加
	intset.Encoding = encoding
	return nil
}

func (intset *Intset) convertTypeWithEnconding(encoding uint, numptr unsafe.Pointer) unsafe.Pointer {
	var ptr unsafe.Pointer
	switch encoding {
	case INTSET_ENC_INT8:
		var numInt8 int8
		numInt8 = *(*int8)(numptr)
		ptr = unsafe.Pointer(&numInt8)
	case INTSET_ENC_INT16:
		var numInt16 int16
		numInt16 = *(*int16)(numptr)
		ptr = unsafe.Pointer(&numInt16)
	case INTSET_ENC_INT32:
		var numInt32 int32
		numInt32 = *(*int32)(numptr)
		ptr = unsafe.Pointer(&numInt32)
	case INTSET_ENC_INT64:
		var numInt64 int64
		numInt64 = *(*int64)(numptr)
		ptr = unsafe.Pointer(&numInt64)
	}
	return ptr
}

// findValue 根据给定的值查找value
func (intset *Intset) findValue(numptr unsafe.Pointer) (bool, int) {
	start := 0
	end := intset.Length - 1
	// 先从中间开始比较
	// 如果num1==num2直接返回
	// 如果num1>num2, 并且前一个依然num3>num2，则在数组的左端查找
	// 如果num1<num2, 并且后一个num3>num2, 直接返回，否则在数组后半段查找
	for start < end {
		mid := (end + start) / 2
		cutnNumPtr := intset.contents[mid]
		curNum := *(*int64)(cutnNumPtr)
		targetNum := *(*int64)(numptr)
		if curNum == targetNum {
			return true, mid
		}

		if curNum < targetNum {
			if mid == intset.Length-1 {
				// 已经查找到最后一个元素，没有发现值
				return false, mid
			}
			nextNum := *(*int64)(intset.contents[mid+1])
			if nextNum > targetNum {
				// 查找到插入的位置
				return false, mid
			}
			end = mid + 1
		}
		if curNum > targetNum {
			if mid == 0 {
				// 已经查找到第一个元素，没有发现值
				return false, mid
			}
			preNum := *(*int64)(intset.contents[mid-1])
			if preNum < targetNum {
				// 查找到插入的位置
				return false, mid
			}
			start = mid - 1
		}
	}
	return false, -1
}

func (intset *Intset) moveWithIndex(index int) {
	if intset.Length < 2 && index == intset.Length-1 {
		return
	}
	for i := intset.Length - 1; i > index; i-- {
		temp := intset.contents[i]
		intset.contents[i] = intset.contents[i-1]
		intset.contents[i-1] = temp
	}
}

// 删除不支持降级操作
func (intset *Intset) deleteWithIndex(index int) error {
	// 原版redis中已经升级的数值不进行降级
	// 把后边的元素向前移动覆盖需要删除的元素
	// 然后释放最后一个空间

	// 在go中由于自身管理底层的连续空间，这里只把最后一个元素设置为nil
	if index < 0 || index > intset.Length-1 {
		return &Error{Code: OUT_RANGE, MSG: "index out arr len"}
	}
	for i := index; i < intset.Length-1; i++ {
		intset.contents[i] = intset.contents[i+1]
	}
	intset.Length--
	if intset.contentsLen-intset.Length >= SET_MAX_VALUE {
		// 没有使用的空间过大时, 生成新的切片
		intset.contents = intset.contents[: intset.Length-1 : intset.Length-1]
	}
	intset.contentsLen = intset.Length
	return nil
}

func IntSetNew() *Intset {
	return &Intset{
		Encoding: INTSET_ENC_INT8,
		Length:   0,
		contents: []unsafe.Pointer{},
	}
}

// IntSetAdd 将元素添加到整数集合中
// error=nil 添加成功 error != nil 根据错误处理即可
// 时间复杂度O(N) + O(logN)
func IntSetAdd(intset *Intset, value unsafe.Pointer, enconding uint) error {
	if intset == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
	}

	if value == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "num ptr nil"}
	}

	// 二分法查找类似O(logN)
	isExist, index := intset.findValue(value)
	if isExist {
		return &Error{Code: VALUE_EXIST, MSG: "value exist"}
	}
	// 直接存储O(1)
	if err := intset.saveNumWithEncoding(enconding, value); err != nil {
		return err
	}
	if index > -1 {
		// 需要根据index，把值交换到对应的位置，确保contens内部的值是排序的O(n)
		intset.moveWithIndex(index)
	}
	return nil
}

// IntsetRemove 在整数集合中删除
// 如果value不存在，返回错误
// 如果存在直接删除对应的value
// T=O(logn+n)
func IntsetRemove(intset *Intset, value unsafe.Pointer) error {
	if intset == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
	}

	if value == nil {
		return &Error{Code: OBJ_PTR_NIL, MSG: "num ptr nil"}
	}
	// 采用二分法变种查找 O(logn)
	isExist, index := intset.findValue(value)
	if !isExist {
		return &Error{Code: VALUE_NOT_FOUND, MSG: "value not exist"}
	}
	// 删除是后边的覆盖前面O(n)
	return intset.deleteWithIndex(index)
}

// IntsetFind 检查给定值是否在整数集合中
// T=O(logn) 时间复杂度
func IntsetFind(intset *Intset, value unsafe.Pointer, enconding uint) (bool, error) {
	if intset == nil {
		return false, &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
	}

	if value == nil {
		return false, &Error{Code: OBJ_PTR_NIL, MSG: "num ptr nil"}
	}

	// 如果编码大于当前的编码则一定不再整数集合中
	if enconding > intset.Encoding {
		return false, nil
	}

	// 采用二分法的变种进行查找 T=O(logn)
	isExist, _ := intset.findValue(value)
	return isExist, nil

}

// IntsetRandom 从整数集合中随机返回一个元素 O(1)
func IntsetRandom(intset *Intset) (num int64, err error) {
	if intset == nil {
		err = &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
		return
	}
	if intset.Length == 0 {
		return
	}
	index := rand.Intn(intset.Length)
	num = *(*int64)(intset.contents[index])
	return
}

// IntsetGet 根据指定位置获取元素 O(1)
func IntsetGet(intset *Intset, index int) (num int64, err error) {
	if intset == nil {
		err = &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
		return
	}

	if index < 0 || index > intset.Length-1 {
		err = &Error{Code: OUT_RANGE, MSG: "index out intset length"}
		return

	}

	num = *(*int64)(intset.contents[index])
	return
}

// IntsetLen 返回整数集合的长度
func IntsetLen(intset *Intset) (length int, err error) {
	if intset == nil {
		err = &Error{Code: OBJ_PTR_NIL, MSG: "intset ptr nil"}
		return
	}
	length = intset.Length
	return
}

// IntsetBlobLen 返回集合占用的总字节数
func IntsetBlobLen(intset *Intset) int {
	if intset == nil {
		return 0
	}
	setLen := int(unsafe.Sizeof(unsafe.Pointer(intset)))
	contenLen := cap(intset.contents) * int(intset.Encoding)
	size := setLen + contenLen
	return size
}
