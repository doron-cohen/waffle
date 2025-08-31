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

func TestActionBuilder_MultipleErrors(t *testing.T) {
	engine := waffle.NewEngine(nil)

	// Create multiple errors in the builder
	err := engine.
		On("test").
		Concurrency(0).                                                 // negative limit
		ConcurrencyGroup("", 1, func(_ context.Context, _ any) string { // empty group name
			return "test"
		}).
		ConcurrencyGroup("valid", 1, nil). // nil key function
		Do("test", func(_ context.Context, _ any) error {
			return nil
		})

	require.Error(t, err)

	// Should contain all three error messages
	errorMsg := err.Error()
	require.Contains(t, errorMsg, "Concurrency: limit must be non-negative")
	require.Contains(t, errorMsg, "groupName must be provided")
	require.Contains(t, errorMsg, "keyFunc must be provided")

	// Should contain comma separators
	require.Contains(t, errorMsg, ", ")
}

func TestActionBuilder_NegativeConcurrency(t *testing.T) {
	engine := waffle.NewEngine(nil)

	err := engine.
		On("test").
		Concurrency(0).
		Do("test", func(_ context.Context, _ any) error {
			return nil
		})

	require.Error(t, err)
	require.Contains(t, err.Error(), "Concurrency: limit must be non-negative")
}

func TestActionBuilder_ErrorDoesNotRegisterAction(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine(nil)

	// Try to register with invalid configuration
	err := engine.
		On("test").
		Concurrency(0).
		Do("test", func(_ context.Context, _ any) error {
			counter.Add(1)
			return nil
		})

	require.Error(t, err)

	// Action should not be registered, so sending event should not trigger it
	ran := engine.Send(t.Context(), "test", nil)
	require.False(t, ran)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(0), counter.Load())
}

func TestErrBuilderBadParams_Error(t *testing.T) {
	// Test single error
	err := &waffle.ErrBuilderBadParams{
		Errors: []error{fmt.Errorf("test error")},
	}
	require.Equal(t, "builder errors: test error", err.Error())

	// Test multiple errors
	err = &waffle.ErrBuilderBadParams{
		Errors: []error{
			fmt.Errorf("first error"),
			fmt.Errorf("second error"),
			fmt.Errorf("third error"),
		},
	}
	expected := "builder errors: first error, second error, third error"
	require.Equal(t, expected, err.Error())

	// Test empty errors
	err = &waffle.ErrBuilderBadParams{
		Errors: []error{},
	}
	require.Equal(t, "builder errors", err.Error())
}

func TestErrBuilderBadParams_Unwrap(t *testing.T) {
	originalErrors := []error{
		fmt.Errorf("error 1"),
		fmt.Errorf("error 2"),
	}

	err := &waffle.ErrBuilderBadParams{
		Errors: originalErrors,
	}

	unwrapped := err.Unwrap()
	require.Equal(t, originalErrors, unwrapped)
	require.Len(t, unwrapped, 2)
	require.Equal(t, "error 1", unwrapped[0].Error())
	require.Equal(t, "error 2", unwrapped[1].Error())
}

func TestErrBuilderBadParams_Is(t *testing.T) {
	err := &waffle.ErrBuilderBadParams{
		Errors: []error{fmt.Errorf("test error")},
	}

	// Test that it implements the error interface
	var _ error = err

	// Test that it can be type asserted
	var builderErr *waffle.ErrBuilderBadParams
	require.ErrorAs(t, err, &builderErr)
	require.NotNil(t, builderErr)
}
