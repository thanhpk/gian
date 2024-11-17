package gian

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

var CHUNKSIZE = 4096 // 4kb
const ONEGB = 1 * 1024 * 1024 * 1024

// self healing file
type Gian struct {
	dead     bool
	filepath string

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

func NewGian(filepath string) *Gian {
	fmt.Println("NEW", filepath)
	me := &Gian{
		filepath:       filepath,
		uncommitBuffer: make([]byte, CHUNKSIZE),

		lastReadIndex: 0,
		readBuffer:    make([]byte, CHUNKSIZE),
	}
	go me.autoCommit()
	return me
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

func (g *Gian) Validate(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}

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
			return err
		}
		crc.Write(lastChecksumB[:])
		crc.Write(indexb[:])

		index := int(binary.BigEndian.Uint64(indexb[:]))
		if index != lastIndex+1 {
			return errors.New("wrong index")
		}
		lastIndex = index

		if _, err := file.Read(lenb[:]); err != nil {
			return err
		}
		crc.Write(lenb[:])

		l := binary.BigEndian.Uint32(lenb[:])
		if l > ONEGB { // 1GB {
			// wrong length -> broken file
			return errors.New("wrong length 3")
		}

		if int(l) > len(data) {
			data = make([]byte, int(l))
		}
		n, err := file.Read(data[:l])
		if err != nil {
			return err
		}
		if n != int(l) {
			return errors.New("wrong len")
		}

		crc.Write(data[:l])
		crc.Write(lenb[:])
		if _, err := file.Read(lenb[:]); err != nil {
			return err
		}
		l2 := binary.BigEndian.Uint32(lenb[:])
		if l2 != l { // 1GB {
			return errors.New("wrong length")
		}

		if _, err := file.Read(checksumb[:]); err != nil {
			return err
		}

		checksum := binary.BigEndian.Uint32(checksumb[:])
		if checksum != crc.Sum32() {
			return errors.New("wrong check sum")
		}
		copy(lastChecksumB[:], checksumb[:])
	}

	return nil
}

// force fix
func (g *Gian) Fix() error {
	fileErr := g.Validate(g.filepath)
	bakFileErr := g.Validate(g.filepath + ".bak")
	if fileErr == nil && bakFileErr == nil {
		return nil
	}

	if fileErr == nil && bakFileErr != nil {
		return CopyFile(g.filepath+".bak", g.filepath)
	}

	if fileErr != nil && bakFileErr == nil {
		return CopyFile(g.filepath, g.filepath+".bak")
	}

	fileErr = g.Validate(g.filepath)
	bakFileErr = g.Validate(g.filepath + ".bak")
	if fileErr == nil && bakFileErr == nil {
		return nil
	}

	fmt.Println("EEEEEE", fileErr, bakFileErr)
	return errors.New("cannot fix." + g.filepath)
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

func (g *Gian) commit(data []byte) error {
	if !g.loaded {
		if err := g.Validate(g.filepath); err != nil {
			if err := g.Fix(); err != nil {
				return err
			}
		}

		file, err := os.OpenFile(g.filepath, os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		rr := NewRReaderSize(file, 1024)
		b4 := [4]byte{}
		n, err := rr.Read(b4[:])
		if err != nil && err != io.EOF {
			return err
		}
		checksum := binary.BigEndian.Uint32(b4[:])
		g.lastCheckSum = checksum
		g.lastWriteIndex = 0
		// not empty file
		if n != 0 {
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
		g.loaded = true
	}

	if len(data) == 0 {
		return nil
	}

	file, err := os.OpenFile(g.filepath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	bakfile, err := os.OpenFile(g.filepath+".bak", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
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
	f, err := os.Open(g.filepath)
	if err != nil && !os.IsNotExist(err) {
		return err
		// log.Err("subiz", err, "CANNOT READ INDEXFIE", indexfile)
	}

	if f == nil {
		f, err = os.OpenFile(g.filepath, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
			// return log.EServer(err, log.M{"account_id": accid, "collection": col, "db": db, "filename": filename})
		}
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

func (g *Gian) readToIndex(toindex int) error {
	if toindex == 0 {
		return nil
	}
	if g.file == nil {
		g.openFile()
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

func (g *Gian) Read() ([]byte, error) {
	if g.file == nil {
		g.openFile()
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
