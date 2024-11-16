package gian

import (
	_ "fmt"
	"io"
	"os"
)

// RReader implements buffering for an io.Reader object.
type RReader struct {
	buf     []byte
	file    *os.File // reader provided by the client
	r       int      // buf readable region [0, r)
	filecur int64    // current position of the file
	err     error
}

// NewReaderSize returns a new [Reader] whose buffer has at least the specified
// size. If the argument io.Reader is already a [Reader] with large enough
// size, it returns the underlying [Reader].
func NewRReaderSize(file *os.File, size int) *RReader {
	if size <= 0 {
		size = 4096
	}
	n, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	return &RReader{
		filecur: n,
		buf:     make([]byte, size),
		file:    file,
	}
}

// NewReader returns a new [Reader] whose buffer has the default size.
func NewRReader(file *os.File) *RReader {
	return NewRReaderSize(file, 4096)
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying [Reader],
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// If the underlying [Reader] can return a non-zero count with io.EOF,
// then this Read method can do so as well; see the [io.Reader] docs.
func (b *RReader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, b.err
	}
	// can read from left-over buffer
	if len(p) <= b.r {
		copy(p, b.buf[b.r-len(p):b.r])
		b.r -= len(p)
		return len(p), nil
	}

	// require read more
	nread := 0         // number of byte have been read
	byteLeft := len(p) // number of byte that need read
	if b.r > 0 {
		copy(p[len(p)-b.r:], b.buf[:b.r])
		byteLeft = len(p) - b.r
		nread += b.r
		b.r = 0
	}

	// nothing more to read from file
	if b.filecur == 0 {
		b.err = io.EOF
	}

	if b.err != nil {
		if nread > 0 {
			return nread, nil
		}
		return 0, b.err
	}

	pos := b.filecur - int64(len(b.buf)) // we will seek the file to this position before read
	nFileReadByte := len(b.buf)          // number of byte going to read from file
	if byteLeft > len(b.buf) {
		pos = b.filecur - int64(byteLeft)
		nFileReadByte = byteLeft
	}

	if pos < 0 {
		pos = 0
		nFileReadByte = int(b.filecur)
		b.err = io.EOF
	}

	fc, err := b.file.Seek(pos, io.SeekStart)
	if err != nil {
		return 0, err
	}
	b.filecur = fc

	if byteLeft > len(b.buf) {
		// fast path for large read, read directly to avoid copy
		n, err := b.file.Read(p[byteLeft-nFileReadByte : byteLeft])
		b.err = err
		return n, err
	}

	n, err = b.file.Read(b.buf[len(b.buf)-nFileReadByte : len(b.buf)])
	if err != nil {
		return n, err
	}
	b.r = len(b.buf) - min(byteLeft, n)

	copy(p[byteLeft-min(byteLeft, n):byteLeft], b.buf[b.r:])
	if len(b.buf)-n > 0 { // shift to left to remove pad bytes
		h := len(b.buf) - n
		b.buf = b.buf[h:b.r]
		b.r -= h
	}
	return nread + min(byteLeft, n), nil
}
