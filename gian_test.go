package gian

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
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

	// messUpFile(filename)
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
	if err := gian.Validate(filename); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestDetectCorrupt(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := NewGian(filename)
	b := [4]byte{}
	num := uint32(12039485)
	binary.BigEndian.PutUint32(b[:], num)
	gian.Write(b[:])
	gian.Close()

	// prepend 1 random byte to file
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	dat = append([]byte{123}, dat...)
	if err := os.WriteFile(filename, dat, 0x777); err != nil {
		panic(err)
	}

	gian = NewGian(filename)
	if _, err := gian.Read(); err == nil {
		t.Errorf("MUST BE ERR")
	} else {
		fmt.Println("ER", err)
	}
}

func TestReadOne(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := NewGian(filename)
	b := [4]byte{}
	num := uint32(12039485)
	binary.BigEndian.PutUint32(b[:], num)
	gian.Write(b[:])
	gian.Close()

	gian = NewGian(filename)
	out, err := gian.Read()
	if err != nil {
		return
	}

	num2 := binary.BigEndian.Uint32(out[:])
	if num2 != num {
		t.Errorf("SHOULDEQ, got %d, want %d", num2, num)
	}

	if err := gian.Validate(filename); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
	fmt.Println("ERR", err)
}

func TestReadWriteManySmallx(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	// defer os.Remove(filename)
	// defer os.Remove(filename + ".bak")

	gian := NewGian(file.Name())
	const N = 2

	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}

	for i := 0; i < N; i++ {
		b, err := gian.Read()
		if err != nil {
			t.Errorf("ERR %d %v", i, err)
			return
		}
		readi := binary.BigEndian.Uint32(b[:])
		if int(readi) != N-i-1 {
			t.Errorf("SHOULDEQ, got %d, want %d", readi, N-i-1)
		}
	}

	if err := gian.Validate(filename); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
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
		if err != nil {
			panic(err)
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

	if err := gian.Validate(file.Name()); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
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

	if err := gian.Validate(file.Name()); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
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
	if err := gian.Validate(file.Name()); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func checkSumFile(filename string) string {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// randomly change byte
// file should large
func messUpFile(filename string) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)

	}

	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	if len(dat) == 0 {
		return
	}

	// fuckup random (up to) 100 bytes
	for i := 0; i < 100; i++ {
		loc := rand.Intn(len(dat) + 1)
		if loc > 1 {
			loc -= 2
		}
		dat[loc] = dat[loc]*2 + byte(i)
	}

	// dupplicate some byte, remove somebyte
	newdata := []byte{}
	for _, b := range dat {
		if rand.Intn(len(dat))%10 == 0 {
			newdata = append(newdata, b)
			newdata = append(newdata, b)
		} else {
			newdata = append(newdata, b)
		}
	}
	if err := os.WriteFile(filename, newdata, 0x777); err != nil {
		panic(err)
	}

	file.Close()
}

func cutFileHead(filename string, length int) {
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	if len(dat) < length {
		if err := os.WriteFile(filename, []byte{}, 0x777); err != nil {
			panic(err)
		}
	}

	dat = dat[length:]
	if err := os.WriteFile(filename, dat, 0x777); err != nil {
		panic(err)
	}
}

func cutFileTail(filename string, length int) {
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	if len(dat) < length {
		if err := os.WriteFile(filename, []byte{}, 0x777); err != nil {
			panic(err)
		}
	}

	dat = dat[:len(dat)-length]
	if err := os.WriteFile(filename, dat, 0x777); err != nil {
		panic(err)
	}
}

func TestHealingFromBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := NewGian(filename)
	N := 10000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()
	cs := checkSumFile(filename + ".bak")
	os.Remove(filename) // remove file
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != cs {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename, 100)
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != cs {
		t.Errorf("MUST HEAL")
	}

	messUpFile(filename)
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != cs {
		t.Errorf("MUST HEAL")
	}

	cutFileTail(filename, 100)
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != cs {
		t.Errorf("MUST HEAL")
	}
}

func TestHealingBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := NewGian(filename)
	N := 10000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()

	os.Remove(filename + ".bak") // remove backup file
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename+".bak", 100)
	if err := gian.Fix(); err != nil {
		panic(err)
	}
	if err := gian.Fix(); err != nil {
		panic(err)
	}
	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	messUpFile(filename + ".bak")
	if err := gian.Fix(); err != nil {
		panic(err)
	}
	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileTail(filename+".bak", 100)
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}
}

/*


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
