package myredis

// redis 用于保存字符串对象的结构 在redis中使用字符串的结构均使用此对象
type SDS struct {
	Len  int    // buf的空间长度 获取长度为O(1)
	Free int    // buf中有多少未被使用 获取字符串长度Len-Free O(1)
	Buf  []byte // 保存字符串对象
}

// 创建一个包含给定字符串的SDS
func (s *SDS) SDSNew(str string) {
	if str == "" {
		return
	}
	strLen := len(str)
	// 检测空间是否足够
	// 如果足够直接存储
	// 如果不足需要对空间进行扩展
	// 扩展之后的长度为 [(s.Len - s.Free) + len(str)] * 2 + 1
	// 最后一个分配的字符串用于保存'\0'此策略为空间预分配
	if s.Free-1 < strLen {
		// 需要扩展的空间长度
		bufLen := ((s.Len-s.Free)+strLen)*2 + 1
		if s.Len+(strLen-s.Free) > 1024*1024 {
			// 如果修改之后的长度>1MB，需要再分配1MB的空间
			bufLen = (s.Len - s.Free) + strLen + 1024*1024 + 1
		}
		s.appenBuf(bufLen)
	}
	for char := range str {
		index := s.Len - s.Free - 1
		s.Buf[index] = byte(char)
		s.Free--
	}
}

// 在一个给定字符串上拼接新的字符串
func (s *SDS) SDSCat(str string) {
	s.SDSNew(str)
}

// appenBuf 扩展buf的空间
func (s *SDS) appenBuf(bufLen int) {
	// 由于goalng的slice分配有底层的规则，这里使用重新赋值来模拟这一情况
	newBuf := make([]byte, bufLen)
	for i := 0; i < (s.Len - s.Free); i++ {
		newBuf[i] = s.Buf[i]
	}
	s.Buf = newBuf
	s.Free = bufLen - (s.Len - s.Free)
	s.Len = bufLen
}

// 获取空间已经使用的字节数
func (s *SDS) SDSLen() int {
	return s.Len
}

// 获取空间中没有被使用的字节数
func (s *SDS) SDSVail() int {
	return s.Free
}

// 保留给定sds的区间内的字符，不在区间内的会被清除 时间复杂度O(N)
func (s *SDS) SDSRange(start, end int) {
	charIndex := s.Len - s.Free - 1 - 1
	if start > charIndex || end > charIndex || start >= end {
		// 检测边界条件
		return
	}

	if end-start == s.Len-s.Free-1 {
		// 没有对原字符串进行任何裁剪
		return
	}

	// 由于惰性分配策略其他空间依旧保留
	for i := start; i <= charIndex; i++ {
		index := i
		var value byte
		if i <= end {
			index = i - start
			value = s.Buf[i]
		}
		s.Buf[index] = value
	}
	s.Free += end - start
}

// 从SDS两端删除给定的字符串 时间复杂度O(M*N)
func (s *SDS) SDSTrim(str string) {
	strLen := len(str)
	strArr := []byte(str)

	if strLen > s.Len-s.Free-1 {
		return
	}
	leftSame := true
	rightSame := true
	for index, char := range strArr {
		if s.Buf[index] != char {
			leftSame = false
		}

		if s.Buf[s.Len-s.Free-1-index] != char {
			rightSame = false
		}
	}

	if !leftSame && !rightSame {
		return
	}

	start := 0
	if leftSame {
		start = strLen - 1
	}
	end := s.Len - s.Free - 1
	if rightSame {
		end = s.Len - s.Free - 1 - strLen
	}

	if end <= start {
		end = s.Len - s.Free - 1
	}
	s.SDSRange(start, end)
}
