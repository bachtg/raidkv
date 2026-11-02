package engine_test

import (
	"testing"

	"github.com/bachtg/raidkv/internal/engine"
	"github.com/bachtg/raidkv/internal/engine/enginetest"
)

func TestMemEngine(t *testing.T) {
	enginetest.Run(t, func(t *testing.T) engine.Engine {
		return engine.NewMemEngine()
	})
}
