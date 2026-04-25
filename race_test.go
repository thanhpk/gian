package gian

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestConcurrentWrite(t *testing.T) {
	file, _ := os.CreateTemp("", "concurrent_write_*.dat")
	filename := file.Name()
	defer os.Remove(filename)
	defer os.Remove(filename + ".bak")

	g := New(filename)
	var wg sync.WaitGroup
	N := 100
	M := 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < M; j++ {
				g.Write([]byte(fmt.Sprintf("worker-%d-msg-%d", id, j)))
			}
		}(i)
	}
	wg.Wait()
	g.ForceCommit()
	g.Close()
}
