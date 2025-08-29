package waffle_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doron-cohen/waffle"
	"github.com/stretchr/testify/require"
)

func TestEngine_Send(t *testing.T) {
	ran := false

	engine := waffle.NewEngine()

	// Register action for event
	engine.On("test").Do(func(_ context.Context, _ any) error {
		ran = true
		return nil
	})

	started := engine.Send(t.Context(), "test", nil)
	require.True(t, started)

	time.Sleep(100 * time.Millisecond)

	require.True(t, ran)
}

func TestEngine_SendWithData(t *testing.T) {
	data := ""

	engine := waffle.NewEngine()

	engine.On("test").Do(func(_ context.Context, d any) error {
		var ok bool
		data, ok = d.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", d)
		}
		return nil
	})

	started := engine.Send(t.Context(), "test", "some data")
	require.True(t, started)

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, "some data", data)
}

func TestEngine_SendMultiple(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.On("test").Do(func(_ context.Context, _ any) error {
		counter.Add(1)
		return nil
	})

	// Send multiple events
	ran1 := engine.Send(t.Context(), "test", nil)
	ran2 := engine.Send(t.Context(), "test", nil)

	require.True(t, ran1)
	require.True(t, ran2)

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, int32(2), counter.Load())
}
