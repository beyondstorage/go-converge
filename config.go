package stream

import (
	"fmt"

	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/panjf2000/ants/v2"
)

const (
	PersistMethodWrite     = "write"
	PersistMethodMultipart = "multipart"
	PersistMethodAppend    = "append"
)

type Config struct {
	Upper types.Storager
	Under types.Storager

	PersistMethod string
}

func New(upper, under types.Storager) (s *Stream, err error) {
	return NewWithConfig(&Config{
		Upper:         upper,
		Under:         under,
		PersistMethod: PersistMethodMultipart,
	})
}

func NewWithConfig(cfg *Config) (s *Stream, err error) {
	s = &Stream{
		method: cfg.PersistMethod,
		upper:  cfg.Upper,
		under:  cfg.Under,
	}

	// Validate persist method.
	switch cfg.PersistMethod {
	case PersistMethodMultipart:
		m, ok := cfg.Under.(types.Multiparter)
		if !ok {
			return nil, fmt.Errorf("under storage %s doesn't support persis method multipart", cfg.Under)
		}
		s.underMultipart = m
	// TODO: we will support appender later.
	case PersistMethodWrite:
		break
	default:
		return nil, fmt.Errorf("not supported persis method: %v", cfg.PersistMethod)
	}

	// FIXME: we will support setting workers later.
	s.p, err = ants.NewPool(10)
	if err != nil {
		return nil, fmt.Errorf("init stream: %w", err)
	}

	// No buffer channel for op.
	s.ch = make(chan op)
	// A sized buffer channel for error.
	s.errch = make(chan error, 10)
	return s, nil
}
