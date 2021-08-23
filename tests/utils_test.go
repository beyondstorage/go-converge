package tests

import (
	"os"
	"testing"

	_ "github.com/beyondstorage/go-service-memory"
	_ "github.com/beyondstorage/go-service-s3/v2"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"

	"github.com/beyondstorage/go-stream"
)

func setup(t *testing.T) (s *stream.Stream, upper, under types.Storager) {
	upperStore, err := services.NewStoragerFromString(os.Getenv("STREAM_UPPER_STORAGE"))
	if err != nil {
		t.Fatalf("init upper storage: %v", err)
	}
	underStore, err := services.NewStoragerFromString(os.Getenv("STREAM_UNDER_STORAGE"))
	if err != nil {
		t.Fatalf("init under storage: %v", err)
	}

	s, err = stream.NewWithConfig(&stream.Config{
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

	return s, upperStore, underStore
}
