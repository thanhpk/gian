package gian

import (
	"bytes"
	"encoding/binary"
	_ "fmt"
	"io"
	"os"
	"testing"
)

func TestLayout(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(file.Name())
	defer os.Remove(filename + ".bak")
	gian := NewGian(filename)
	gian.Write([]byte("hello"))
	gian.ForceCommit()
	gian.Write([]byte("goodbye"))
	gian.ForceCommit()
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	mustbe := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // stt 1
		0x00, 0x00, 0x00, 0x05, // length 1
		0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
		0x00, 0x00, 0x00, 0x05, // length 1
		0xb5, 0x71, 0x54, 0x7d, // checksum 1 (00000000|0000000000000001|00000005656c6c6f00000005S)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // stt 1
		0x00, 0x00, 0x00, 0x07, // length 2
		0x67, 0x6f, 0x6f, 0x64, 0x62, 0x79, 0x65, // goodbuye
		0x00, 0x00, 0x00, 0x07,
		0x68, 0x66, 0x73, 0xc3, // length 2
	}
	if !bytes.Equal(dat, mustbe) {
		t.Errorf("SHOULDBEEQ, %v, %v, \n%x\n%x", len(dat), len(mustbe), dat, mustbe)
	}
}

func TestReadWriteManySmall(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := NewGian(file.Name())
	for i := range 1000 {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}

	for i := 0; i < 1000; i++ {
		b, err := gian.Read()
		if err != nil {
			t.Errorf("ERR %d %v", i, err)
			return
		}
		readi := binary.BigEndian.Uint32(b[:])
		if int(readi) != 1000-i-1 {
			t.Errorf("SHOULDEQ, got %d, want %d", readi, 1000-i-1)
		}
	}
}

func TestReadWriteManySmallCommit(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := NewGian(file.Name())
	const N = 100000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}

	gian.ForceCommit()
	out := []byte{}
	for {
		b, err := gian.Read()
		if err == io.EOF {
			break
		}
		tmp := append([]byte{}, b...)
		out = append(tmp, out...)
	}

	for i := 0; i < N; i++ {
		i32 := binary.BigEndian.Uint32(out[i*4 : i*4+4])
		if i != int(i32) {
			t.Errorf("SHOULDEQ, got %d, want %d", i32, i)
		}
	}
}

func TestReadWriteBig(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := NewGian(file.Name())
	big := []byte{}
	N := 100000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		big = append(big, b[:]...)
	}
	gian.Write(big) // must commit since the byte is so big

	big = []byte{}
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i*2))
		big = append(big, b[:]...)
	}
	gian.Write(big) // must commit since the byte is so big

	// first read
	readb, err := gian.Read()
	if err != nil {
		panic(err)
	}
	for i := 0; i < N; i++ {
		i32 := binary.BigEndian.Uint32(readb[i*4 : i*4+4])
		if i*2 != int(i32) {
			t.Errorf("SHOULDEQ, got %d, want %d", i32, i)
		}
	}

	// second read
	readb, err = gian.Read()
	if err != nil {
		panic(err)
	}
	for i := 0; i < N; i++ {
		i32 := binary.BigEndian.Uint32(readb[i*4 : i*4+4])
		if i != int(i32) {
			t.Errorf("SHOULDEQ, got %d, want %d", i32, i)
		}
	}
}

func TestReadWriteSmallBigMix(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := NewGian(file.Name())
	const N = 100000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}

	big := []byte{}
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(N+i))
		big = append(big, b[:]...)
	}
	gian.Write(big) // must commit since the byte is so big

	// first read
	readb, err := gian.Read()
	if err != nil {
		panic(err)
	}
	for i := 0; i < N; i++ {
		i32 := binary.BigEndian.Uint32(readb[i*4 : i*4+4])
		if N+i != int(i32) {
			t.Errorf("SHOULDEQ, got %d, want %d", i32, i)
		}
	}

	out := []byte{}
	for {
		b, err := gian.Read()
		if err == io.EOF {
			break
		}
		tmp := append([]byte{}, b...)
		out = append(tmp, out...)
	}

	for i := 0; i < N; i++ {
		i32 := binary.BigEndian.Uint32(out[i*4 : i*4+4])
		if i != int(i32) {
			t.Errorf("SHOULDEQ, got %d, want %d", i32, i)
		}
	}
}

/*
func TestCommit(t *testing.T) {

}



func TestHealingFromBackup(t *testing.T) {
	file, _ := ioutil.TempFile("", "*.dat")
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	filename := file.Name()
	gian := NewGian(filename)
	for i := range 1000 {
		gian.Write([]byte(i))
	}
	gian.Commit()

	os.Remote(filename) // remove backup file
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename)
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	messUpFile(filename)
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}
}

func checkSumFile(filename string) string {}

// randomly change byte
func messUpFile(filename string) {
}

func cutFileHead(filename string, length int64) {
}

func cutFileTail(filename string, length int64) {
}

func TestHealingBackup(t *testing.T) {
	file, _ := ioutil.TempFile("", "*.dat")
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	filename := file.Name()
	gian := NewGian(filename)
	for i := range 1000 {
		gian.Write([]byte(i))
	}
	gian.Commit()

	os.Remote(filename + ".bak") // remove backup file
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileInHalf(filename + ".bak")
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	messUpFile(filename + ".bak")
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}
}

func TestHealingBothMainAndBackup(t *testing.T) {
	file, _ := ioutil.TempFile("", "*.dat")
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	filename := file.Name()
	gian := NewGian(filename)
	for i := range 1000 {
		gian.Write([]byte(i))
	}
	gian.Commit()

	cutFileHead(filename, 40)
	cutFileTail(filename+".bak", 50)
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename+".bak", 40)
	cutFileTail(filename, 50)
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename+".bak", 40)
	cutFileHead(filename, 50)
	if err := gian.Fix(filename); err != nil {
		panic(err)
	}
	if err := gian.Fix(filename); err != nil {
		t.Errorf("CANNOT FIX, MUST ERR")
	}
}

func TestWriteToBrokenFile(t *testing.T) {
	file, err := ioutil.TempFile("dir", "myname.*.bat")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())
	gian := NewGian(file.Name())
	for i := range 1000 {
		gian.Write([]byte(i))
	}

	gian.Commit()

}

*/
