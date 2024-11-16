package gian

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

var CHUNKSIZE = 4096 // 4kb
const ONEGB = 1 * 1024 * 1024 * 1024

// self healing file
type Gian struct {
	filepath string

	// writing
	lastCheckSum   uint32
	lastWriteIndex int
	loaded         bool

	uncommitLength int
	uncommitBuffer []byte

	file *os.File
	rr   *RReader

	// reading
	lastReadIndex int
	readBuffer    []byte

	dead bool
}

func NewGian(filepath string) *Gian {
	fmt.Println("NEW", filepath)
	me := &Gian{
		filepath:       filepath,
		uncommitBuffer: make([]byte, CHUNKSIZE),

		lastReadIndex: 0,
		readBuffer:    make([]byte, CHUNKSIZE),

		dead: false,
	}
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

func (g *Gian) validate(filepath string) bool {
	return true
}

// force fix
func (g *Gian) Fix() error {
	return g.selfHealing()

}
func (g *Gian) fixUp(filepath, bakfilepath string) bool {
	// moving for
	for true {
		// read num file 1

	}
	return true
}

func (g *Gian) selfHealing() error {
	// main file broken
	if !g.validate(g.filepath) {
	}

	// backup file roken
	if !g.validate(g.filepath + ".bak") {
	}
	return nil
}

func (g *Gian) commit(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	// validate first

	file, err := os.OpenFile(g.filepath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
		// return log.EServer(err, log.M{"account_id": accid, "collection": col, "db": db, "filename": filename})
	}

	bakfile, err := os.OpenFile(g.filepath+".bak", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
		// return log.EServer(err, log.M{"account_id": accid, "collection": col, "db": db, "filename": filename})
	}

	crc := crc32.NewIEEE()
	lastchecksumb := [4]byte{}
	binary.BigEndian.PutUint32(lastchecksumb[:], g.lastCheckSum)
	// fmt.Println("KKKKK", g.lastCheckSum, g.lastWriteIndex+1)

	indexB := [8]byte{}
	binary.BigEndian.PutUint64(indexB[:], uint64(g.lastWriteIndex+1))

	lengthB := [4]byte{}
	binary.BigEndian.PutUint32(lengthB[:], uint32(len(data)))

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

func (g *Gian) Read() ([]byte, error) {
	if g.file == nil {
		g.openFile()
	}
	if g.lastReadIndex == -1 {
		return nil, io.EOF
	}

	checksumb := [4]byte{}
	if _, err := g.rr.Read(checksumb[:]); err != nil {
		return nil, err
	}
	lenb := [4]byte{}
	if _, err := g.rr.Read(lenb[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenb[:])
	if l > ONEGB { // 1GB {
		// wrong length -> broken file

	}

	readBuffer := g.readBuffer
	if int(l) > len(g.readBuffer) {
		readBuffer = make([]byte, l)
	}

	if _, err := g.rr.Read(readBuffer[:l]); err != nil {
		return nil, err
	}

	lenb2 := [4]byte{}
	if _, err := g.rr.Read(lenb2[:]); err != nil {
		return nil, err
	}
	l2 := binary.BigEndian.Uint32(lenb2[:])
	if l2 != l {
		// wrong length -> broken file -> fix up
	}

	indexb := [8]byte{}
	if _, err := g.rr.Read(indexb[:]); err != nil {
		return nil, err
	}

	index := int(binary.BigEndian.Uint64(indexb[:]))
	if g.lastReadIndex == 0 {
		g.lastReadIndex = int(index)
	} else {
		if index != g.lastReadIndex {
			// broken file -> fix up
		}
	}

	return readBuffer[0:l], nil
}
