package waffle_test

import (
	"context"
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
		Do(func(_ context.Context) error {
			ran = true
			return nil
		}).
		Build()
	require.NoError(t, err)

	started := engine.Send(t.Context(), "test")
	require.True(t, started)

	time.Sleep(100 * time.Millisecond)

	require.True(t, ran)
}
