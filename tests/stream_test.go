package tests

import (
	"bytes"
	"os"
	"testing"

	_ "github.com/beyondstorage/go-service-memory"
	_ "github.com/beyondstorage/go-service-s3/v2"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/stretchr/testify/assert"

	"github.com/beyondstorage/go-stream"
)

func TestStream(t *testing.T) {
	upperStore, err := services.NewStoragerFromString(os.Getenv("STREAM_UPPER_STORAGE"))
	if err != nil {
		t.Fatalf("init upper storage: %v", err)
	}
	underStore, err := services.NewStoragerFromString(os.Getenv("STREAM_UNDER_STORAGE"))
	if err != nil {
		t.Fatalf("init under storage: %v", err)
	}

	s, err := stream.NewWithConfig(&stream.Config{
		Upper:         upperStore,
		Under:         underStore,
		PersistMethod: stream.PersistMethodMultipart,
	})
	if err != nil {
		t.Fatalf("init stream: %v", err)
	}
	go s.Serve()
	go func() {
		for v := range s.Errors() {
			t.Logf("got error: %v", v)
		}
	}()

	// A bytes with 1k.
	bs := bytes.Repeat([]byte{'a'}, 1024)

	cases := []struct {
		name  string
		count int
	}{
		{"1MB", 1024},
		{"16MB", 16 * 1024},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			br, err := s.StartBranch(1, "abc")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < v.count; i++ {
				_, err = br.Write(uint64(i), bs)
				if err != nil {
					t.Fatal(err)
				}
			}
			err = br.Complete()
			if err != nil {
				t.Fatal(err)
			}

			var actualContent bytes.Buffer
			_, err = underStore.Read("abc", &actualContent)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, bytes.Repeat(bs, v.count), actualContent.Bytes())
		})
	}
}
