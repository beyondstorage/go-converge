package tests

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStreamWrite(t *testing.T) {
	s, _, under := setup(t)

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
			name := uuid.NewString()

			br, err := s.StartBranch(rand.Uint64(), name)
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
			_, err = under.Read(name, &actualContent)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, bytes.Repeat(bs, v.count), actualContent.Bytes())
		})
	}
}

func TestStreamReadFrom(t *testing.T) {
	s, _, under := setup(t)

	cases := []struct {
		name string
		size int
	}{
		{"1MB", 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			name := uuid.NewString()

			// A bytes with size.
			bs := bytes.Repeat([]byte{'a'}, v.size)

			br, err := s.StartBranch(rand.Uint64(), name)
			if err != nil {
				t.Fatal(err)
			}

			n, err := br.ReadFrom(bytes.NewReader(bs))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, n, int64(v.size))

			err = br.Complete()
			if err != nil {
				t.Fatal(err)
			}

			var actualContent bytes.Buffer
			_, err = under.Read(name, &actualContent)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, bs, actualContent.Bytes())
		})
	}
}
