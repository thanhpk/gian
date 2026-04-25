package gian

import (
	"crypto/rand"
	"os"
	"testing"
)

func BenchmarkWriteSmall(b *testing.B) {
	file, _ := os.CreateTemp("", "bench_write_small_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	g := New(filename)
	data := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Write(data)
	}
	g.ForceCommit()
	g.Close()
}

func BenchmarkWriteLarge(b *testing.B) {
	file, _ := os.CreateTemp("", "bench_write_large_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	g := New(filename)
	data := make([]byte, 1024*1024) // 1MB
	rand.Read(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Write(data)
	}
	g.ForceCommit()
	g.Close()
}

func BenchmarkRead(b *testing.B) {
	file, _ := os.CreateTemp("", "bench_read_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	g := New(filename)
	data := make([]byte, 4096)
	rand.Read(data)
	N := 1000
	for i := 0; i < N; i++ {
		g.Write(data)
	}
	g.ForceCommit()
	g.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g2 := New(filename)
		for {
			_, err := g2.Read()
			if err != nil {
				break
			}
		}
		g2.Close()
	}
}
