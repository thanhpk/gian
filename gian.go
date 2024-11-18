package gian

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"time"
)

var CHUNKSIZE = 4096 // 4kb
const ONEGB = 1 * 1024 * 1024 * 1024

// self healing file
type Gian struct {
	dead     bool
	filename string

	// writing
	lastCheckSum   uint32
	lastWriteIndex int
	loaded         bool

	uncommitLength int
	uncommitBuffer []byte

	// reading
	file              *os.File
	rr                *RReader
	lastReadCheckSumB [4]byte
	lastReadIndex     int
	readBuffer        []byte
}

func New(filename string) *Gian {
	if filename == "" {
		file, _ := os.CreateTemp("/tmp", "*.dat")
		filename = file.Name()
	}

	me := &Gian{
		filename:       filename,
		uncommitBuffer: make([]byte, CHUNKSIZE),
		readBuffer:     make([]byte, CHUNKSIZE),
	}
	go me.autoCommit()
	return me
}

func (g *Gian) GetFileName() string {
	return g.filename
}

func (g *Gian) Close() error {
	err := g.ForceCommit()
	if g.file != nil {
		g.file.Close()
	}
	g.dead = true
	return err
}

func (g *Gian) Write(data []byte) error {
	if g.uncommitLength > 0 && len(data)+g.uncommitLength > CHUNKSIZE {
		if err := g.commit(g.uncommitBuffer[:g.uncommitLength]); err != nil {
			return err
		}
		g.uncommitLength = 0
	}

	if len(data) > CHUNKSIZE {
		return g.commit(data)
	}
	copy(g.uncommitBuffer[g.uncommitLength:g.uncommitLength+len(data)], data)
	g.uncommitLength += len(data)
	return nil
}

func (g *Gian) Fix() error {
	findex, _, fileErr := ReadFromStart(g.filename, false)
	bindex, _, bakFileErr := ReadFromStart(g.filename+".bak", false)
	if findex == bindex && fileErr == nil && bakFileErr == nil {
		return nil
	}

	if findex != bindex && fileErr == nil && bakFileErr == nil {
		if findex < bindex {
			return CopyFile(g.filename, g.filename+".bak")
		}
		return CopyFile(g.filename+".bak", g.filename)
	}
	headIndex, headdata, _ := ReadFromStart(g.filename, true)
	bakheadIndex, bakheaddata, _ := ReadFromStart(g.filename+".bak", true)
	if headIndex < bakheadIndex {
		headIndex = bakheadIndex
		headdata = bakheaddata
	}

	pass, taildata := LoadBackwardToIndex(g.filename, headIndex)
	if !pass {
		pass, taildata = LoadBackwardToIndex(g.filename+".bak", headIndex)
	}
	if !pass {
		return errors.New("cannot fix. " + g.filename)
	}

	fixed := append(headdata, taildata...)
	if err := os.WriteFile(g.filename, fixed, 0644); err != nil {
		return err
	}
	if err := os.WriteFile(g.filename+".bak", fixed, 0644); err != nil {
		return err
	}

	findex, _, fileErr = ReadFromStart(g.filename, false)
	bindex, _, bakFileErr = ReadFromStart(g.filename+".bak", false)
	if findex == bindex && fileErr == nil && bakFileErr == nil {
		return nil
	}

	if findex != bindex && fileErr == nil && bakFileErr == nil {
		if findex > bindex {
			return CopyFile(g.filename, g.filename+".bak")
		}
		return CopyFile(g.filename+".bak", g.filename)
	}

	return errors.New("cannot fix.." + g.filename)
}

func (g *Gian) autoCommit() {
	time.Sleep(30 * time.Second)
	for !g.dead {
		time.Sleep(30 * time.Second)
		if g.uncommitLength > 0 {
			g.ForceCommit()
		}
	}
}

func mustInsync(f1, f2 string) error {
	i1, _, err1 := ReadFromStart(f1, false)
	i2, _, err2 := ReadFromStart(f2, false)

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
			if err := g.Fix(); err != nil {
				return err
			}
		}

		file, err := os.OpenFile(g.filename, os.O_RDONLY, 0644)
		if err == nil {
			defer file.Close()
			b4 := [4]byte{}
			rr := NewRReaderSize(file, 1024)
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

	file, err := os.OpenFile(g.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	bakfile, err := os.OpenFile(g.filename+".bak", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
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

	// [ N ] [ Length ] [ --- data ---- ] [ Length ] [ CHECKSUM ]
	file.Write(indexB[:])
	bakfile.Write(indexB[:])

	file.Write(lengthB[:])
	bakfile.Write(lengthB[:])

	file.Write(data[:])
	bakfile.Write(data[:])

	file.Write(lengthB[:])
	bakfile.Write(lengthB[:])

	file.Write(checksumB[:])
	bakfile.Write(checksumB[:])

	g.lastWriteIndex++
	g.lastCheckSum = checksum
	file.Close()
	bakfile.Close()
	return nil
}

func (g *Gian) ForceCommit() error {
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

	f, err := os.OpenFile(g.filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if f == nil {
		panic("wtf" + err.Error())
	}
	g.file = f
	g.rr = NewRReaderSize(f, CHUNKSIZE)
	return nil
}

func (g *Gian) fixThenRead(reason string) ([]byte, error) {
	if err := g.Fix(); err != nil {
		return nil, err
	}
	g.file = nil
	if err := g.readToIndex(g.lastReadIndex); err != nil {
		return nil, err
	}
	return g.Read()
}

func ReadFromStart(filename string, readdata bool) (int, []byte, error) {
	// makeSurePath(filename)
	out := []byte{}
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return 0, nil, err
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
			return lastIndex, out, err
		}
		crc.Write(lastChecksumB[:])
		crc.Write(indexb[:])
		index := int(binary.BigEndian.Uint64(indexb[:]))

		if index != lastIndex+1 {
			return lastIndex, out, errors.New("wrong index 234908234")
		}
		if _, err := file.Read(lenb[:]); err != nil {
			return lastIndex, out, err
		}
		crc.Write(lenb[:])

		l := binary.BigEndian.Uint32(lenb[:])
		if l > ONEGB { // 1GB {
			return lastIndex, out, errors.New("wrong length 3")
		}

		if int(l) > len(data) {
			data = make([]byte, int(l))
		}
		n, err := file.Read(data[:l])
		if err != nil {
			return lastIndex, out, err
		}
		if n != int(l) {
			return lastIndex, out, err
		}

		crc.Write(data[:l])
		crc.Write(lenb[:])
		if _, err := file.Read(lenb[:]); err != nil {
			return lastIndex, out, err
		}
		l2 := binary.BigEndian.Uint32(lenb[:])
		if l2 != l { // 1GB {
			return lastIndex, out, errors.New("wrong len")
		}

		if _, err := file.Read(checksumb[:]); err != nil {
			return lastIndex, out, err
		}

		checksum := binary.BigEndian.Uint32(checksumb[:])
		if checksum != crc.Sum32() {
			return lastIndex, out, errors.New("wrong check sum")
		}
		lastIndex = index
		if readdata {
			out = append(out, indexb[:]...)
			out = append(out, lenb[:]...)
			out = append(out, data[:l]...)
			out = append(out, lenb[:]...)
			out = append(out, checksumb[:]...)
		}
		copy(lastChecksumB[:], checksumb[:])
	}

	return lastIndex, out, nil
}

// the return data do not include headIndex
// (headIndex...end]
func LoadBackwardToIndex(filename string, headIndex int) (bool, []byte) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return false, nil
	}
	defer file.Close()
	rr := NewRReaderSize(file, 1024)

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
		return false, nil
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
			break
		}
		// do check sum
		if index > 1 {
			if _, err := rr.Read(prevchecksumb[:]); err != nil {
				return false, nil
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
			return false, nil
		}

		if lastReadIndex != 0 {
			if index+1 != lastReadIndex {
				return false, nil
			}
		}
		lastReadIndex = int(index)

		ele := []byte{}
		ele = append([]byte{}, indexb[:]...)
		ele = append(ele, lenb[:]...)
		ele = append(ele, data[:]...)
		ele = append(ele, lenb[:]...)
		ele = append(ele, checksumb[:]...)
		out = append(out, ele)
		copy(checksumb[:], prevchecksumb[:])
		if lastReadIndex == headIndex {
			break
		}
	}

	if lastReadIndex <= headIndex+1 {
		res := []byte{}
		for i := len(out) - 1; i >= 0; i-- {
			res = append(res, out[i]...)
		}
		return true, res
	}
	return false, nil
}

func (g *Gian) readToIndex(toindex int) error {
	if toindex == 0 {
		return nil
	}
	if g.file == nil {
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
	makeSurePath(newname)
	os.Remove(newname)
	return os.Rename(g.filename, newname)
}

func (g *Gian) ReadAll() ([]byte, error) {
	out := []byte{}
	for {
		data, err := g.Read()
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
	if g.file != nil {
		g.file.Close()
		g.file = nil
	}
}

func (g *Gian) Read() ([]byte, error) {
	if g.file == nil {
		if err := g.openFile(); err != nil {
			return nil, err
		}
		// read first checksum
		if _, err := g.rr.Read(g.lastReadCheckSumB[:]); err != nil {
			return nil, err
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
		//		return nil, errors.New("wrong length, broken file")
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
			// has extra byte in the begging of the file
			// return nil, errors.New("begining corrupted")
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

	if g.lastReadIndex == 0 {
	} else {
		if index+1 != g.lastReadIndex {
			return g.fixThenRead("wrong index")
		}
	}
	g.lastReadIndex = int(index)

	return data, nil
}

// copyFileContents copies the contents of the file named src to the file named
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
