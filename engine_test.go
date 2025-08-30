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
	engine.On("test").Do("test", func(_ context.Context, _ any) error {
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

	engine.On("test").Do("test", func(_ context.Context, d any) error {
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

	engine.On("test").Do("test", func(_ context.Context, _ any) error {
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

func TestEngine_DifferentActionsForEvent(t *testing.T) {
	ran1 := false
	ran2 := false

	engine := waffle.NewEngine()

	engine.On("test").Do("test1", func(_ context.Context, _ any) error {
		ran1 = true
		return nil
	})

	engine.On("test").Do("test2", func(_ context.Context, _ any) error {
		ran2 = true
		return nil
	})

	engine.Send(t.Context(), "test", nil)

	time.Sleep(100 * time.Millisecond)

	require.False(t, ran1)
	require.True(t, ran2)
}

func TestEngine_OneActionForMultipleEvents(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.On("test1", "test2").Do("test", func(_ context.Context, _ any) error {
		counter.Add(1)
		return nil
	})

	engine.Send(t.Context(), "test1", nil)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(1), counter.Load())

	engine.Send(t.Context(), "test2", nil)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(2), counter.Load())
}

func TestEngine_ConcurrencyLimit(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.
		On("test").
		Concurrency(1).
		Do("test", func(_ context.Context, _ any) error {
			counter.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test", nil)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), counter.Load())
}

func TestEngine_ConcurrencyLimit_MultipleActions(t *testing.T) {
	counter1 := atomic.Int32{}
	counter2 := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.
		On("test").
		Concurrency(1).
		Do("test", func(_ context.Context, _ any) error {
			counter1.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	engine.On("test2").
		Do("test2", func(_ context.Context, _ any) error {
			counter2.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test", nil)
	engine.Send(t.Context(), "test2", nil)
	engine.Send(t.Context(), "test2", nil)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), counter1.Load())
	require.Equal(t, int32(2), counter2.Load())
}

func TestEngine_ConcurrencyGroup_Basic(t *testing.T) {
	users := make([]string, 0, 3)

	engine := waffle.NewEngine()

	engine.
		On("test").
		ConcurrencyGroup("user", 1, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(_ context.Context, data any) error {
			users = append(users, data.(string))
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	// Send events with same user ID - should be limited
	engine.Send(t.Context(), "test", "user1")
	engine.Send(t.Context(), "test", "user1")

	// Send events with different user IDs - should both run
	engine.Send(t.Context(), "test", "user2")
	engine.Send(t.Context(), "test", "user3")

	time.Sleep(200 * time.Millisecond)
	require.ElementsMatch(t, []string{"user1", "user2", "user3"}, users)
}

func TestEngine_ConcurrencyGroup_MultipleGroupsWithSameKey(t *testing.T) {
	counter := atomic.Int32{}
	users := make([]string, 0, 3)

	engine := waffle.NewEngine()

	engine.
		On("test").
		ConcurrencyGroup("userA", 2, func(_ context.Context, data any) string {
			return data.(string)
		}).
		ConcurrencyGroup("userB", 1, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(_ context.Context, data any) error {
			counter.Add(1)
			users = append(users, data.(string))
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	// Send events that should be limited by both groups
	engine.Send(t.Context(), "test", "user1")
	engine.Send(t.Context(), "test", "user1") // blocked by user group
	engine.Send(t.Context(), "test", "user2") // should run
	engine.Send(t.Context(), "test", "user2") // blocked by user group

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(2), counter.Load())
	require.ElementsMatch(t, []string{"user1", "user2"}, users)
}

func TestEngine_ConcurrencyGroup_WithGlobalLimit(t *testing.T) {
	counter := atomic.Int32{}
	users := make([]string, 0, 2)

	engine := waffle.NewEngine()

	engine.
		On("test").
		Concurrency(2). // global limit of 2
		ConcurrencyGroup("user", 1, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(_ context.Context, data any) error {
			counter.Add(1)
			users = append(users, data.(string))
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	// Send events - global limit should allow 2, but user group should further limit
	engine.Send(t.Context(), "test", "user1") // runs
	engine.Send(t.Context(), "test", "user1") // blocked by user group
	engine.Send(t.Context(), "test", "user2") // runs (within global limit)
	engine.Send(t.Context(), "test", "user2") // blocked by global limit

	time.Sleep(200 * time.Millisecond)
	require.ElementsMatch(t, []string{"user1", "user2"}, users)
	require.Equal(t, int32(2), counter.Load())
}

func TestEngine_ConcurrencyGroup_KeyFunctionNil(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	// Test with nil key function - should use empty string as key
	engine.
		On("test").
		ConcurrencyGroup("global", 1, nil).
		Do("test", func(_ context.Context, _ any) error {
			counter.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	engine.Send(t.Context(), "test", "data1")
	engine.Send(t.Context(), "test", "data2") // should be blocked

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), counter.Load())
}

func TestEngine_ConcurrencyGroup_ComplexData(t *testing.T) {
	type UserRequest struct {
		UserID string
		Action string
	}

	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.
		On("process").
		ConcurrencyGroup("user", 1, func(_ context.Context, data any) string {
			return data.(UserRequest).UserID
		}).
		ConcurrencyGroup("action", 2, func(_ context.Context, data any) string {
			return data.(UserRequest).Action
		}).
		Do("process", func(_ context.Context, _ any) error {
			counter.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		})

	// Test different combinations
	engine.Send(t.Context(), "process", UserRequest{UserID: "user1", Action: "read"})
	engine.Send(t.Context(), "process", UserRequest{UserID: "user1", Action: "read"})  // blocked by user
	engine.Send(t.Context(), "process", UserRequest{UserID: "user2", Action: "read"})  // should run
	engine.Send(t.Context(), "process", UserRequest{UserID: "user2", Action: "write"}) // should run
	engine.Send(t.Context(), "process", UserRequest{UserID: "user2", Action: "write"}) // blocked by action

	time.Sleep(150 * time.Millisecond)
	require.Equal(t, int32(2), counter.Load())
}

func TestEngine_ContextCancellation(t *testing.T) {
	counter := atomic.Int32{}
	ctx, cancel := context.WithCancel(t.Context())

	engine := waffle.NewEngine()

	engine.
		On("test").
		ConcurrencyGroup("user", 1, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(ctx context.Context, _ any) error {
			counter.Add(1)
			// Simulate long-running task
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

	// Send first event
	engine.Send(ctx, "test", "user1")

	// Cancel context while first event is running
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Try to send another event - should run since the first run is cancelled
	engine.Send(t.Context(), "test", "user2")

	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(2), counter.Load())
}

func TestEngine_ConcurrencyGroup_ZeroLimit(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	engine.
		On("test").
		ConcurrencyGroup("blocked", 0, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(_ context.Context, _ any) error {
			counter.Add(1)
			return nil
		})

	// Send event - should not run due to zero limit
	engine.Send(t.Context(), "test", "user1")

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(0), counter.Load())
}

func TestEngine_ConcurrencyGroup_EmptyGroupName(t *testing.T) {
	counter := atomic.Int32{}

	engine := waffle.NewEngine()

	// Empty group name should work as a regular group
	engine.
		On("test").
		ConcurrencyGroup("", 1, func(_ context.Context, data any) string {
			return data.(string)
		}).
		Do("test", func(_ context.Context, _ any) error {
			counter.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		})

	engine.Send(t.Context(), "test", "user1")
	engine.Send(t.Context(), "test", "user1") // should be blocked

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), counter.Load())
}
