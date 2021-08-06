package stream

import "github.com/beyondstorage/go-storage/v4/types"

type Config struct {
	Upper types.Storager
	Under types.Storager
}

func New(upper, under types.Storager) (con *Stream, err error) {
	return
}

func NewWithConfig(cfg *Config) (con *Stream, err error) {
	return
}
