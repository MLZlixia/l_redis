package utils

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
