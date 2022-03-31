package main

import (
	"fmt"
	"unsafe"
)

type Myname struct {
	Name string
}

type Do1 struct{}

func main() {
	// var numInt32 int32
	// num := 2 << 3
	// fmt.Println(num)
	// var numInt32 int32 = int32(num)
	// fmt.Println(numInt32)
	// numInt32 = int32(num)
	// var numInt8 int8
	// numInt8 = *(*int8)(unsafe.Pointer(&numInt32))
	// fmt.Println(numInt8)

	// var name *Myname
	// *(*Do1)(unsafe.Pointer(name)) = Do1{}
	// fmt.Println(name)

	// a := []int{1, 2, 3, 4, 5}
	// c := a[0:3]
	// a = append(a, 3)
	// fmt.Print(c)

	// a := []byte{}
	// for i := 0; i < 65535; i++ {
	// 	a = append(a, byte(i))
	// }

	// for i := 0; i < 1000; i++ {
	// 	fmt.Println(a[i])
	// }
	var num uint32 = 682363284
	zl := make([]byte, 4)
	saveZlBytes(zl, uint32(num))
	fmt.Println(getZlBytes(zl) == num)

	ptr := *(*int64)(unsafe.Pointer(&zl))
	fmt.Println(ptr)
	fmt.Println(unsafe.ArbitraryType(ptr))
}

const (
	// 用于表示zlBytes所占的字节数
	bytesLength = 4
	// 字节和位的转换 1字节=8bit
	bitCarry = 8
)

func saveZlBytes(zl []byte, zlBytes uint32) {
	flag := uint32(1<<bitCarry) - 1
	for bytesIndex := bytesLength - 1; bytesIndex >= 0 && zlBytes > 0; bytesIndex-- {
		num := zlBytes & flag
		zl[bytesIndex] = byte(num)
		zlBytes = zlBytes >> bitCarry
	}
}

func getZlBytes(zl []byte) uint32 {
	var num uint32
	var carry uint = 0
	for index := bytesLength - 1; index >= 0; index-- {
		num += uint32(zl[index]) << carry
		carry += bitCarry
	}
	return num
}
