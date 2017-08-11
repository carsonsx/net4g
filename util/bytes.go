package util

func BytesIndexOf(b, sub []byte) int {
	for i := 0; i < len(b)-len(sub)+1; i++ {
		found := true
		for j := 0; j < len(sub); j++ {
			if b[i+j] != sub[j] {
				found = false
				break
			}
		}
		if found {
			return i
		}
	}
	return -1
}

func CombineBytes(one []byte, others ...[]byte) []byte {
	l := len(one)
	for _, other := range others {
		if other == nil {
			continue
		}
		l += len(other)
	}
	newBytes := make([]byte, l)
	copy(newBytes, one)
	l = len(one)
	for _, other := range others {
		if other == nil {
			continue
		}
		copy(newBytes[l:], other)
		l += len(other)
	}
	return newBytes
}