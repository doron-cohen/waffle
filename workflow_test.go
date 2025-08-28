package waffle_test

import (
	"context"
	"testing"

	"github.com/doron-cohen/waffle"
	"github.com/stretchr/testify/require"
)

func TestWorkflowBuilder(t *testing.T) {
	key := ""
	err := waffle.NewWorkflowBuilder(
		"test",
		func(eventKey string, workflow waffle.Workflow) error {
			key = eventKey

			return nil
		},
	).Do(func(_ context.Context) error {
		return nil
	}).Build()

	require.NoError(t, err)
	require.Equal(t, "test", key, "event key should be set")
}
