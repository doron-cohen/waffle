package waffle_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/doron-cohen/waffle"
	"github.com/stretchr/testify/require"
)

func TestEngine_Send(t *testing.T) {
	ran := false

	engine := waffle.NewEngine()
	err := engine.
		On("test").
		Do(func(_ context.Context, _ any) error {
			ran = true
			return nil
		}).
		Build()
	require.NoError(t, err)

	started := engine.Send(t.Context(), "test", nil)
	require.True(t, started)

	time.Sleep(100 * time.Millisecond)

	require.True(t, ran)
}

func TestEngine_SendWithData(t *testing.T) {
	data := ""

	engine := waffle.NewEngine()
	err := engine.
		On("test").
		Do(func(_ context.Context, d any) error {
			var ok bool
			data, ok = d.(string)
			if !ok {
				return fmt.Errorf("expected string, got %T", d)
			}

			return nil
		}).
		Build()
	require.NoError(t, err)

	started := engine.Send(t.Context(), "test", "some data")
	require.True(t, started)

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, "some data", data)
}
