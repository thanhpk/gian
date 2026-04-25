package gian

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/thanhpk/vdisk"
)

const DEFAULT_CHUNKSIZE = 4096 // 4kb
const ONEGB = 1 * 1024 * 1024 * 1024

// self healing file
type Gian struct {
	mu       sync.Mutex
	stopOnce sync.Once
	stopChan chan struct{}

	filename string

	// writing
	lastCheckSum   uint32
	lastWriteIndex int
	loaded         bool

	chunkSize      int
	uncommitLength int
	uncommitBuffer []byte

	wfile    *os.File
	wbakfile *os.File

	// reading
	rfile             *vdisk.File
	rr                *RReader
	lastReadCheckSumB [4]byte
	lastReadIndex     int
	readBuffer        []byte

	limitReadMbs float64
}

func New(filename string) *Gian {
	if filename == "" {
		file, _ := os.CreateTemp("", "gian_*.dat")
		filename = file.Name()
	}

	me := &Gian{
		filename:       filename,
		chunkSize:      DEFAULT_CHUNKSIZE,
		uncommitBuffer: make([]byte, DEFAULT_CHUNKSIZE),
		readBuffer:     make([]byte, DEFAULT_CHUNKSIZE),
		limitReadMbs:   100_000, //  ~ 100Gbs/s -> no limit
		stopChan:       make(chan struct{}),
	}
	go me.autoCommit()
	return me
}

func NewWithReadLimit(filename string, limitReadMbs float64) *Gian {
	me := New(filename)
	if limitReadMbs > 0 {
		me.limitReadMbs = limitReadMbs
	}
	return me
}

func (g *Gian) GetFileName() string {
	return g.filename
}

func (g *Gian) Close() error {
	g.stopOnce.Do(func() {
		close(g.stopChan)
	})

	g.mu.Lock()
	defer g.mu.Unlock()

	err := g.forceCommit()
	if g.rfile != nil {
		g.rfile.Close()
	}
	if g.wfile != nil {
		g.wfile.Close()
	}
	if g.wbakfile != nil {
		g.wbakfile.Close()
	}
	return err
}

func (g *Gian) Write(data []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.uncommitLength > 0 && len(data)+g.uncommitLength > g.chunkSize {
		if err := g.commit(g.uncommitBuffer[:g.uncommitLength]); err != nil {
			return err
		}
		g.uncommitLength = 0
	}

	if len(data) > g.chunkSize {
		return g.commit(data)
	}
	copy(g.uncommitBuffer[g.uncommitLength:g.uncommitLength+len(data)], data)
	g.uncommitLength += len(data)
	return nil
}

func (g *Gian) Fix() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fix()
}

func (g *Gian) fix() error {
	if g.wfile != nil {
		g.wfile.Close()
		g.wfile = nil
	}
	if g.wbakfile != nil {
		g.wbakfile.Close()
		g.wbakfile = nil
	}
	if g.rfile != nil {
		g.rfile.Close()
		g.rfile = nil
	}

	findex, _ := ReadFromStart(g.filename, nil)
	bindex, _ := ReadFromStart(g.filename+".bak", nil)

	// Even if findex == bindex, we might need to truncate junk at the end
	// of both files to ensure Read() doesn't keep hitting it.

	tmpFile, err := os.CreateTemp("", "gian_fix_*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	headIndex := findex
	headFile := g.filename
	if bindex > findex {
		headIndex = bindex
		headFile = g.filename + ".bak"
	}

	// Copy the healthy head
	if _, err := ReadFromStart(headFile, tmpFile); err != nil {
		// It's okay if it hits corruption, we just want the healthy part
	}

	// Try to find a tail from either file that connects to this head
	pass, _ := LoadBackwardToIndex(g.filename, headIndex, tmpFile)
	if !pass {
		pass, _ = LoadBackwardToIndex(g.filename+".bak", headIndex, tmpFile)
	}

	if headIndex == 0 && !pass {
		return errors.New("cannot fix: both files corrupted from the start")
	}

	if err := tmpFile.Sync(); err != nil {
		return err
	}

	// Overwrite both files with the fixed content
	if err := CopyFile(g.filename, tmpFile.Name()); err != nil {
		return err
	}
	if err := CopyFile(g.filename+".bak", tmpFile.Name()); err != nil {
		return err
	}

	return nil
}

func (g *Gian) autoCommit() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			g.mu.Lock()
			if g.uncommitLength > 0 {
				g.forceCommit()
			}
			g.mu.Unlock()
		case <-g.stopChan:
			return
		}
	}
}

func mustInsync(f1, f2 string) error {
	i1, err1 := ReadFromStart(f1, nil)
	i2, err2 := ReadFromStart(f2, nil)

	if err1 != nil {
		if i2 == 0 {
			return nil
		}
		return err1
	}

	if err2 != nil {
		if i1 == 0 {
			return nil
		}
		return err2
	}
	if i1 == i2 {
		return nil
	}

	return errors.New("backup and bin not in sync")
}

func makeSurePath(filename string) {
	paths := strings.Split(filename, "/")
	if len(paths) > 1 {
		dir := strings.Join(paths[:len(paths)-1], "/")
		os.MkdirAll(dir, os.ModePerm)
	}
}

func (g *Gian) commit(data []byte) error {
	if !g.loaded {
		makeSurePath(g.filename)

		if err := mustInsync(g.filename, g.filename+".bak"); err != nil {
			if err := g.fix(); err != nil {
				return err
			}
		}

		file, err := os.OpenFile(g.filename, os.O_RDONLY, 0644)
		if err == nil {
			defer file.Close()
			b4 := [4]byte{}
			rr, err := NewRReaderSize(file, 1024)
			if err != nil {
				return err
			}
			n, err := rr.Read(b4[:])
			if err != nil && err != io.EOF {
				return err
			}
			// not empty file
			if n != 0 {
				checksum := binary.BigEndian.Uint32(b4[:])
				g.lastCheckSum = checksum
				g.lastWriteIndex = 0

				if _, err := rr.Read(b4[:]); err != nil {
					return err
				}
				l := binary.BigEndian.Uint32(b4[:])
				if l > ONEGB { // 1GB {
					return errors.New("wrong length, very broken")
				}
				b := make([]byte, l)
				if _, err := rr.Read(b); err != nil {
					return err
				}
				if _, err := rr.Read(b4[:]); err != nil {
					return err
				}
				indexb := [8]byte{}
				if _, err := rr.Read(indexb[:]); err != nil {
					return err
				}
				index := int(binary.BigEndian.Uint64(indexb[:]))
				g.lastWriteIndex = index
			}
		} else {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		g.loaded = true
	}
	if len(data) == 0 {
		return nil
	}

	if g.wfile == nil {
		file, err := os.OpenFile(g.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		g.wfile = file
	}

	if g.wbakfile == nil {
		bakfile, err := os.OpenFile(g.filename+".bak", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		g.wbakfile = bakfile
	}

	lastchecksumb := [4]byte{}
	binary.BigEndian.PutUint32(lastchecksumb[:], g.lastCheckSum)

	indexB := [8]byte{}
	binary.BigEndian.PutUint64(indexB[:], uint64(g.lastWriteIndex+1))

	lengthB := [4]byte{}
	binary.BigEndian.PutUint32(lengthB[:], uint32(len(data)))

	crc := crc32.NewIEEE()
	crc.Write(lastchecksumb[:])
	crc.Write(indexB[:])
	crc.Write(lengthB[:])
	crc.Write(data[:])
	crc.Write(lengthB[:])
	checksum := crc.Sum32()
	checksumB := [4]byte{}
	binary.BigEndian.PutUint32(checksumB[:], checksum)

	// Aggregated write for better performance
	buf := make([]byte, 0, 8+4+len(data)+4+4)
	buf = append(buf, indexB[:]...)
	buf = append(buf, lengthB[:]...)
	buf = append(buf, data...)
	buf = append(buf, lengthB[:]...)
	buf = append(buf, checksumB[:]...)

	// [ N ] [ Length ] [ --- data ---- ] [ Length ] [ CHECKSUM ]
	if _, err := g.wfile.Write(buf); err != nil {
		return err
	}
	if _, err := g.wbakfile.Write(buf); err != nil {
		return err
	}

	g.lastWriteIndex++
	g.lastCheckSum = checksum
	return nil
}

func (g *Gian) ForceCommit() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.forceCommit()
}

func (g *Gian) forceCommit() error {
	if g.uncommitLength == 0 {
		return nil // no op
	}
	if err := g.commit(g.uncommitBuffer[:g.uncommitLength]); err != nil {
		return err
	}
	g.uncommitLength = 0
	return nil
}

func (g *Gian) openFile() error {
	makeSurePath(g.filename)
	f, err := vdisk.NewLimiter(g.limitReadMbs).OpenFile(g.filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	rr, err := NewRReaderSize(f, g.chunkSize)
	if err != nil {
		f.Close()
		return err
	}
	g.rfile = f
	g.rr = rr
	return nil
}

func (g *Gian) fixThenRead(reason string) ([]byte, error) {
	if err := g.fix(); err != nil {
		return nil, err
	}
	if g.rfile != nil {
		g.rfile.Close()
		g.rfile = nil
	}
	if err := g.readToIndex(g.lastReadIndex); err != nil {
		return nil, err
	}
	return g.read()
}

func ReadFromStart(filename string, writer io.Writer) (int, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	lastIndex := 0
	crc := crc32.NewIEEE()
	checksumb := [4]byte{}
	lastChecksumB := [4]byte{}
	indexb := [8]byte{}
	lenb := [4]byte{}
	data := make([]byte, 4096)
	for {
		crc.Reset()
		_, err := file.Read(indexb[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return lastIndex, err
		}
		crc.Write(lastChecksumB[:])
		crc.Write(indexb[:])
		index := int(binary.BigEndian.Uint64(indexb[:]))

		if index != lastIndex+1 {
			return lastIndex, errors.New("wrong index")
		}
		if _, err := file.Read(lenb[:]); err != nil {
			return lastIndex, err
		}
		crc.Write(lenb[:])

		l := binary.BigEndian.Uint32(lenb[:])
		if l > ONEGB { // 1GB {
			return lastIndex, errors.New("wrong length 3")
		}

		if int(l) > len(data) {
			data = make([]byte, int(l))
		}
		n, err := file.Read(data[:l])
		if err != nil {
			return lastIndex, err
		}
		if n != int(l) {
			return lastIndex, io.ErrUnexpectedEOF
		}

		crc.Write(data[:l])
		crc.Write(lenb[:])
		if _, err := file.Read(lenb[:]); err != nil {
			return lastIndex, err
		}
		l2 := binary.BigEndian.Uint32(lenb[:])
		if l2 != l {
			return lastIndex, errors.New("wrong len")
		}

		if _, err := file.Read(checksumb[:]); err != nil {
			return lastIndex, err
		}

		checksum := binary.BigEndian.Uint32(checksumb[:])
		if checksum != crc.Sum32() {
			return lastIndex, errors.New("wrong check sum")
		}
		lastIndex = index
		if writer != nil {
			writer.Write(indexb[:])
			writer.Write(lenb[:])
			writer.Write(data[:l])
			writer.Write(lenb[:])
			writer.Write(checksumb[:])
		}
		copy(lastChecksumB[:], checksumb[:])
	}

	return lastIndex, nil
}

// the return data do not include headIndex
// (headIndex...end]
func LoadBackwardToIndex(filename string, headIndex int, writer io.Writer) (bool, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer file.Close()
	rr, err := NewRReaderSize(file, 1024)
	if err != nil {
		return false, err
	}

	readBuffer := []byte{}
	checksumb := [4]byte{}
	prevchecksumb := [4]byte{}
	lenb := [4]byte{}
	indexb := [8]byte{}

	out := [][]byte{}
	var lastReadIndex int

	_, err = rr.Read(checksumb[:])
	if err == io.EOF && headIndex == 0 {
		return true, nil
	}
	if err != nil && err != io.EOF {
		return false, err
	}
	for {
		if _, err := rr.Read(lenb[:]); err != nil {
			break
		}
		l := binary.BigEndian.Uint32(lenb[:])
		if l > ONEGB { // 1GB {
			break
		}
		if int(l) > len(readBuffer) {
			readBuffer = make([]byte, l)
		}
		if _, err := rr.Read(readBuffer[:l]); err != nil {
			break
		}

		if n, err := rr.Read(lenb[:]); err != nil || n != 4 {
			break
		}

		l2 := binary.BigEndian.Uint32(lenb[:])
		if l2 != l {
			break
		}

		if n, err := rr.Read(indexb[:]); err != nil || n != 8 {
			break
		}

		index := int(binary.BigEndian.Uint64(indexb[:]))
		data := readBuffer[0:l]

		if index <= headIndex {
			lastReadIndex = index
			break
		}
		// do check sum
		if index > 1 {
			if _, err := rr.Read(prevchecksumb[:]); err != nil {
				return false, err
			}
		} else {
			prevchecksumb[0], prevchecksumb[1], prevchecksumb[2], prevchecksumb[3] = 0, 0, 0, 0
		}
		crc := crc32.NewIEEE()
		crc.Write(prevchecksumb[:])
		crc.Write(indexb[:])
		crc.Write(lenb[:])
		crc.Write(data[:])
		crc.Write(lenb[:])

		// confirm the checksum

		checksum := binary.BigEndian.Uint32(checksumb[:])
		if checksum != crc.Sum32() {
			return false, errors.New("checksum mismatch")
		}

		if lastReadIndex != 0 {
			if index+1 != lastReadIndex {
				return false, errors.New("index gap")
			}
		}
		lastReadIndex = int(index)

		ele := []byte{}
		ele = append(ele, indexb[:]...)
		ele = append(ele, lenb[:]...)
		ele = append(ele, data[:]...)
		ele = append(ele, lenb[:]...)
		ele = append(ele, checksumb[:]...)
		out = append(out, ele)
		copy(checksumb[:], prevchecksumb[:])
		if lastReadIndex == headIndex+1 {
			break
		}
	}

	if lastReadIndex <= headIndex+1 {
		if writer != nil {
			for i := len(out) - 1; i >= 0; i-- {
				if _, err := writer.Write(out[i]); err != nil {
					return false, err
				}
			}
		}
		return true, nil
	}
	return false, nil
}

func (g *Gian) readToIndex(toindex int) error {
	if toindex == 0 {
		return nil
	}
	if g.rfile == nil {
		if err := g.openFile(); err != nil {
			return err
		}
	}
	readBuffer := []byte{}
	lenb := [4]byte{}

	// skip fist checksum
	if _, err := g.rr.Read(lenb[:]); err != nil {
		return err
	}

	var lastReadIndex int
	for {
		if _, err := g.rr.Read(lenb[:]); err != nil {
			return err
		}
		l := binary.BigEndian.Uint32(lenb[:])
		if l > ONEGB { // 1GB {
			return errors.New("wrong length, very broken")
		}

		if int(l) > len(readBuffer) {
			readBuffer = make([]byte, l)
		}

		if _, err := g.rr.Read(readBuffer[:l]); err != nil {
			return err
		}

		if _, err := g.rr.Read(lenb[:]); err != nil {
			return err
		}

		indexb := [8]byte{}
		if _, err := g.rr.Read(indexb[:]); err != nil {
			return err
		}

		index := int(binary.BigEndian.Uint64(indexb[:]))
		if lastReadIndex != 0 {
			if index+1 != lastReadIndex {
				return errors.New("wrong index, broken file")
			}
		}

		if index < toindex {
			return errors.New("wrong index, VERY broken file")
		}

		// skip checksum
		if _, err := g.rr.Read(lenb[:]); err != nil {
			return err
		}
		lastReadIndex = index
		if index == toindex {
			return nil // ok
		}
	}
}

func (g *Gian) Rename(newname string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	makeSurePath(newname)
	os.Remove(newname)
	return os.Rename(g.filename, newname)
}

func (g *Gian) ReadAll() ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []byte{}
	for {
		data, err := g.read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return out, err
		}

		out = append(out, data...)
	}
	return out, nil
}

func (g *Gian) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.rfile != nil {
		g.rfile.Close()
		g.rfile = nil
	}
}

func (g *Gian) Read() ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.read()
}

func (g *Gian) read() ([]byte, error) {
	if g.rfile == nil {
		if err := g.openFile(); err != nil {
			return nil, err
		}

		// read first checksum
		g.rr.Read(g.lastReadCheckSumB[:])
		bakf, err := os.OpenFile(g.filename+".bak", os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		defer bakf.Close()
		bakrr, err := NewRReaderSize(bakf, g.chunkSize)
		if err != nil {
			return nil, err
		}
		// read first checksum
		bakcsb := [4]byte{}
		bakrr.Read(bakcsb[:])
		if !bytes.Equal(bakcsb[:], g.lastReadCheckSumB[:]) {
			if g.rfile != nil {
				g.rfile.Close()
				g.rfile = nil
			}
			if err := g.fix(); err != nil {
				return nil, err
			}
			return g.read()
		}

		if g.uncommitLength > 0 {
			return g.uncommitBuffer[:g.uncommitLength], nil
		}
	}

	lenb := [4]byte{}
	if _, err := g.rr.Read(lenb[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenb[:])
	if l > ONEGB { // 1GB {
		return g.fixThenRead("wrong len")
	}

	readBuffer := g.readBuffer
	if int(l) > len(g.readBuffer) {
		readBuffer = make([]byte, l)
	}

	if _, err := g.rr.Read(readBuffer[:l]); err != nil {
		return nil, err
	}

	if _, err := g.rr.Read(lenb[:]); err != nil {
		if err == io.EOF {
			return g.fixThenRead("here" + err.Error())
		}
		return nil, err
	}

	l2 := binary.BigEndian.Uint32(lenb[:])
	if l2 != l {
		return g.fixThenRead("wrong len2")
	}

	indexb := [8]byte{}
	if _, err := g.rr.Read(indexb[:]); err != nil {
		return nil, err
	}
	index := int(binary.BigEndian.Uint64(indexb[:]))

	data := readBuffer[0:l]
	if index == 1 {
		// do extra read must be eof
		onebyte := []byte{0}
		if n, _ := g.rr.Read(onebyte[:]); n != 0 {
			return g.fixThenRead("no extra byte")
		}
		return data, nil
	}
	// do check sum
	crc := crc32.NewIEEE()

	prevchecksumb := [4]byte{}
	if _, err := g.rr.Read(prevchecksumb[:]); err != nil {
		return nil, err
	}
	crc.Write(prevchecksumb[:])
	crc.Write(indexb[:])
	crc.Write(lenb[:])
	crc.Write(data[:])
	crc.Write(lenb[:])

	// confirm the checksum
	checksum := binary.BigEndian.Uint32(g.lastReadCheckSumB[:])
	if checksum != crc.Sum32() {
		return g.fixThenRead("wrong checksum")
	}
	g.lastReadCheckSumB = prevchecksumb

	if g.lastReadIndex != 0 {
		if index+1 != g.lastReadIndex {
			return g.fixThenRead("wrong index")
		}
	}
	g.lastReadIndex = int(index)

	return data, nil
}

// CopyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func CopyFile(dst, src string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
