package gian

import (
	"io"
	"os"
	"testing"
	// 	"time"
)

func TestRReader(t *testing.T) {
	testCases := []string{
		`1`,
		`1this`,
		`1this-a-short`,
		`1this-a-short-test-file`,
		`1this-a-short-test-file`,
		`1this-a-short-test-file2`,
		`1this-a-short-test-file21this-a-short-test-file21this-a-short-test-file2`,
	}
	for byteread := range 10 {
		if byteread == 0 {
			continue
		}
		for buffersize := range 10 {
			for _, originalContent := range testCases {
				// prepare file
				file, err := os.CreateTemp("/tmp/", "test")
				if err != nil {
					panic(err)
				}
				file.Write([]byte(originalContent))
				file.Close()
				defer os.Remove(file.Name())

				f, err := os.Open(file.Name())
				if err != nil {
					panic(err)
				}

				reader := NewRReaderSize(f, buffersize)
				var b = make([]byte, byteread)
				content := ""
				for {
					n, err := reader.Read(b)
					if err == io.EOF {
						break
					}
					if err != nil {
						panic(err)
					}
					content = string(b[len(b)-n:]) + content
				}
				if content != originalContent {
					t.Errorf("SHOULD EQ buffer %d, byteread %d. got \n.%x.\n.%x.", buffersize, byteread, []byte(content), []byte(originalContent))
				}
			}
		}
	}
}

//   .74000000680069732d612d73686f72742d746573742d66696c6520.
//           .746869732d612d73686f72742d746573742d66696c6520.
