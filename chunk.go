package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math/bits"
)

var gear = func() (t [256]uint64) {
	for i := range t {
		sum := sha256.Sum256([]byte{byte(i)})
		t[i] = binary.LittleEndian.Uint64(sum[:])
	}
	return
}()

// chunker cuts r into content-defined chunks of avg/4 to 4*avg bytes (FastCDC with normalised chunking level 1).
type chunker struct {
	r             io.Reader
	buf           []byte
	n, used       int
	min, avg, max int
	err           error
}

func newChunker(r io.Reader, avg int) *chunker {
	return &chunker{r: r, buf: make([]byte, 4*avg), min: avg / 4, avg: avg, max: 4 * avg}
}

// next returns the next chunk; the slice is valid until the following call.
func (c *chunker) next() ([]byte, error) {
	c.n = copy(c.buf, c.buf[c.used:c.n])
	for c.n < c.max && c.err == nil {
		var m int
		m, c.err = c.r.Read(c.buf[c.n:])
		c.n += m
	}
	if c.n == 0 {
		return nil, cmp.Or(c.err, io.EOF)
	}
	c.used = c.cut(c.buf[:c.n])
	return c.buf[:c.used], nil
}

func (c *chunker) cut(b []byte) int {
	avgBits := bits.Len(uint(c.avg)) - 1
	var h uint64
	for i := c.min; i < len(b); i++ {
		h = h<<1 + gear[b[i]]
		shift := 64 - (avgBits + 1)
		if i >= c.avg {
			shift = 64 - (avgBits - 1)
		}
		if h>>shift == 0 {
			return i + 1
		}
	}
	return len(b)
}
