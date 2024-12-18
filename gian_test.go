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
	gian := New(filename)
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
	if _, _, err := ReadFromStart(filename, false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestReadFromUncommit(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(filename)
	const N = 10
	b := [4]byte{}
	for i := range N {
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}
	binary.BigEndian.PutUint32(b[:], uint32(N))
	gian.Write(b[:])

	for i := 0; i < N+1; i++ {
		b, err := gian.Read()
		if err != nil {
			t.Errorf("ERR %d %v", i, err)
			return
		}
		readi := binary.BigEndian.Uint32(b[:])
		if int(readi) != N-i {
			t.Errorf("SHOULDEQ, got %d, want %d", readi, N-i)
		}
	}
}

func TestReadAll(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(filename)
	all := []byte{}
	const N = 10
	b := [4]byte{}

	for i := range N {
		binary.BigEndian.PutUint32(b[:], uint32(i+1))
		gian.Write(b[:])
		all = append(append([]byte{}, b[:]...), all...)
		gian.ForceCommit()
	}
	binary.BigEndian.PutUint32(b[:], uint32(N))
	gian.Write(b[:])
	all = append(append([]byte{}, b[:]...), all...)

	out, err := gian.ReadAll()
	if err != nil {
		panic(err)
	}

	if !bytes.Equal(out, all) {
		t.Errorf("SHOULD EQ\n%x\n%x", out, all)
	}
}

func TestDetectCorrupt(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(filename)
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

	gian = New(filename)
	if _, err := gian.Read(); err != nil {
		t.Errorf("MUST BE NO ERR")
	}
}

func TestReadOne(t *testing.T) {
	file, _ := os.CreateTemp("/tmp", "gian_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(filename)
	b := [4]byte{}
	num := uint32(12039485)
	binary.BigEndian.PutUint32(b[:], num)
	gian.Write(b[:])
	gian.Close()

	gian = New(filename)
	out, err := gian.Read()
	if err != nil {
		return
	}

	num2 := binary.BigEndian.Uint32(out[:])
	if num2 != num {
		t.Errorf("SHOULDEQ, got %d, want %d", num2, num)
	}

	if _, _, err := ReadFromStart(filename, false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestReadWriteManySmallx(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(file.Name())
	const N = 1000
	b := [4]byte{}
	for i := range N {
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

	if _, _, err := ReadFromStart(filename, false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestReadFromBrokenFileMissing(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(file.Name())
	const N = 10_000

	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}

	os.Remove(filename) // remove file
	for i := range N {
		b, err := gian.Read()
		if err != nil {
			t.Errorf("ERR %d %v", i, err)
			return
		}
		readi := binary.BigEndian.Uint32(b[:])
		if int(readi) != N-i-1 {
			t.Errorf("SHOULDEQ got %d, want %d", readi, N-i-1)
		}
	}
}

func TestReadFromBrokenFile(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(file.Name())
	const N = 10_000

	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}

	for r := range 100 {
		messUpFile(filename)
		gian = New(filename)
		for i := 0; i < N; i++ {
			b, err := gian.Read()
			if err != nil {
				t.Errorf("RUN %d, ERR %d %v", r, i, err)
				return
			}
			readi := binary.BigEndian.Uint32(b[:])
			if int(readi) != N-i-1 {
				t.Errorf("SHOULDEQ RUN %d, got %d, want %d", r, readi, N-i-1)
			}
		}
	}
}

func TestReadWriteManySmallCommit(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := New(file.Name())
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

	if _, _, err := ReadFromStart(file.Name(), false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestReadWriteBig(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := New(file.Name())
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

	if _, _, err := ReadFromStart(file.Name(), false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestReadWriteSmallBigMix(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := New(file.Name())
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
	if _, _, err := ReadFromStart(file.Name(), false); err != nil {
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

	if len(dat) < length {
		dat = []byte{}
	} else {
		dat = dat[length:]
	}
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

	if len(dat)-length < 0 {
		dat = []byte{}
	} else {
		dat = dat[:len(dat)-length]
	}
	if err := os.WriteFile(filename, dat, 0x777); err != nil {
		panic(err)
	}
}

func appendRandom(filename string, length int) {
	dat, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	if len(dat) < length {
		if err := os.WriteFile(filename, []byte{}, 0x777); err != nil {
			panic(err)
		}
	}

	for i := range length {
		dat = append(dat, byte(i%255))
	}
	if err := os.WriteFile(filename, dat, 0x777); err != nil {
		panic(err)
	}
}

func TestHealingFromBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	gian := New(filename)
	N := 10
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()
	cs := checkSumFile(filename + ".bak")
	cutFileHead(filename, 100)
	if err := gian.Fix(); err != nil {
		panic(err)
	}
	if checkSumFile(filename) != cs {
		t.Errorf("MUST HEAL %s, %s, %s", checkSumFile(filename), cs, checkSumFile(filename+".bak"))
	}
	gian.Close()

	gian = New(filename)
	N = 100_000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()
	cs = checkSumFile(filename + ".bak")
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
		t.Errorf("MUST HEAL %s, %s, %s", checkSumFile(filename), cs, checkSumFile(filename+".bak"))
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
		t.Errorf("MUST HEAL %s  %s  %s", checkSumFile(filename), cs, checkSumFile(filename+".bak"))
	}
}

func TestHealingBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := New(filename)
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

func TestWriteToBrokenFile(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := New(file.Name())
	const N = 1000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()
	gian.Close()
	appendRandom(file.Name(), 10000)
	gian = New(file.Name())
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(N))
	gian.Write(b[:])
	gian.ForceCommit()

	out, err := gian.Read()
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(b[:], out[:]) {
		t.Errorf("MUST EQ, got %x, want %x", out, b)
	}
	out = []byte{}
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

	if _, _, err := ReadFromStart(file.Name(), false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestWriteToBrokenBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	defer os.Remove(file.Name())
	defer os.Remove(file.Name() + ".bak")

	gian := New(file.Name())
	const N = 1000
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
	}
	gian.ForceCommit()
	gian.Close()
	appendRandom(file.Name()+".bak", 10000)

	gian = New(file.Name())
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(N))
	gian.Write(b[:])
	gian.ForceCommit()

	if checkSumFile(file.Name()) != checkSumFile(file.Name()+".bak") {
		t.Errorf("MUST HEAL")
	}

	out, err := gian.Read()
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(b[:], out[:]) {
		t.Errorf("MUST EQ, got %x, want %x", out, b)
	}
	out = []byte{}
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

	if _, _, err := ReadFromStart(file.Name(), false); err != nil {
		t.Errorf("MUST BE TRUE %v", err)
	}
}

func TestLoadForward(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := New(filename)

	index, data, _ := ReadFromStart(filename, true)
	if index != 0 || len(data) != 0 {
		t.Errorf("SHOULD BE 0")
	}

	N := 10
	b := [4]byte{}
	for i := range N {
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}
	gian.Close()
	originb, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	appendRandom(filename, 100)
	index, data, _ = ReadFromStart(filename, true)
	if !bytes.Equal(originb, data) {
		t.Errorf("SHOULD EQ.\n%x\n%x", originb, data)
	}

	if index != 10 {
		t.Errorf("SHOULD BE 10, got %d", index)
	}
}

func TestLoadBackward(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := New(filename)

	pass, data := LoadBackwardToIndex(filename, 0)
	if pass == false {
		t.Errorf("SHOULD BE TRUE")
	}

	if len(data) != 0 {
		t.Errorf("SHOULD BE 0")
	}

	N := 5
	b := [4]byte{}
	for i := range N {
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}
	gian.Close()

	pass, _ = LoadBackwardToIndex(filename, 0)
	if !pass {
		t.Errorf("SHOULD BE TRUE")
	}

	pass, data = LoadBackwardToIndex(filename, N)
	if !pass {
		t.Errorf("SHOULD BE TRUE")
	}

	if len(data) > 0 {
		t.Errorf("SHOULD BE ZERO GOT %d", len(data))
	}

	originb, err := os.ReadFile(filename)
	if err != nil {
		panic(err)

	}

	cutFileHead(filename, 2)
	pass, _ = LoadBackwardToIndex(filename, 1)
	if !pass {
		t.Errorf("SHOULD BE TRUE")
	}

	pass, _ = LoadBackwardToIndex(filename, 0)
	if pass {
		t.Errorf("SHOULD BE FALSE")
	}

	expect := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04,
		0x02, 0xc9, 0xcd, 0x55,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x04,
		0x37, 0xaa, 0x45, 0xc6,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x04,
		0x9b, 0x0a, 0x56, 0xe7,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x04,
		0x6d, 0x34, 0x85, 0x3c,
	}

	pass, data = LoadBackwardToIndex(filename, 1)
	if !pass {
		t.Errorf("SHOULD BE TRUE")
	}

	if !bytes.Equal(data, expect) {
		t.Errorf("SHOULD EQ\n%x\n%x\n%x", data, expect, originb[24:])
	}

	return
	gian = New(filename)
	N = 1
	binary.BigEndian.PutUint32(b[:], 113)
	gian.Write(b[:])
	gian.Close()

	pass, data = LoadBackwardToIndex(filename, 1)
	if !pass {
		t.Errorf("SHOULD BE TRUE")
	}
}

func TestHealingBothMainAndBackup(t *testing.T) {
	file, _ := os.CreateTemp("", "*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")
	gian := New(filename)
	N := 100
	for i := range N {
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(i))
		gian.Write(b[:])
		gian.ForceCommit()
	}

	cutFileHead(filename, 40)
	cutFileTail(filename+".bak", 10)
	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename+".bak", 40)
	cutFileTail(filename, 50)

	if err := gian.Fix(); err != nil {
		panic(err)
	}

	if checkSumFile(filename) != checkSumFile(filename+".bak") {
		t.Errorf("MUST HEAL")
	}

	cutFileHead(filename+".bak", 40)
	cutFileHead(filename, 50)
	if err := gian.Fix(); err == nil {
		t.Errorf("SHOULD BE ERR")
	}
}
