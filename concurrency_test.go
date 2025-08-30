package waffle_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doron-cohen/waffle"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyGroups_AddGlobalLimit(t *testing.T) {
	groups := waffle.NewConcurrencyGroups()

	// Test adding global limit
	groups.AddGlobalLimit(2)

	// Should be able to acquire twice
	acquired1, release1 := groups.TryAcquire(t.Context(), "data1")
	require.True(t, acquired1)

	acquired2, release2 := groups.TryAcquire(t.Context(), "data2")
	require.True(t, acquired2)

	// Third should fail
	acquired3, _ := groups.TryAcquire(t.Context(), "data3")
	require.False(t, acquired3)

	// Release and try again
	release1()
	release2()

	acquired4, _ := groups.TryAcquire(t.Context(), "data4")
	require.True(t, acquired4)
}

func TestConcurrencyGroups_AddGroup(t *testing.T) {
	groups := waffle.NewConcurrencyGroups()

	// Add group with key function
	groups.Add("user", 1, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Same key should be limited
	acquired1, release1 := groups.TryAcquire(t.Context(), "user1")
	require.True(t, acquired1)

	acquired2, _ := groups.TryAcquire(t.Context(), "user1")
	require.False(t, acquired2)

	// Different key should work
	acquired3, _ := groups.TryAcquire(t.Context(), "user2")
	require.True(t, acquired3)

	release1()
}

func TestConcurrencyGroups_ZeroLimit(t *testing.T) {
	groups := waffle.NewConcurrencyGroups()

	// Add zero limit group
	groups.Add("blocked", 0, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Should never acquire
	acquired, _ := groups.TryAcquire(t.Context(), "test")
	require.False(t, acquired)
}

func TestConcurrencyGroups_NoKeyFunc(t *testing.T) {
	groups := waffle.NewConcurrencyGroups()

	// Add group without key function (global per group)
	groups.Add("global", 1, nil)

	// All should use same key
	acquired1, release1 := groups.TryAcquire(t.Context(), "data1")
	require.True(t, acquired1)

	acquired2, _ := groups.TryAcquire(t.Context(), "data2")
	require.False(t, acquired2)

	release1()
}

func TestConcurrencyGroups_MultipleGroups(t *testing.T) {
	groups := waffle.NewConcurrencyGroups()

	// Add global limit
	groups.AddGlobalLimit(2)

	// Add user-specific limit
	groups.Add("user", 1, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Should respect both limits
	acquired1, _ := groups.TryAcquire(t.Context(), "user1")
	require.True(t, acquired1)

	acquired2, _ := groups.TryAcquire(t.Context(), "user1") // blocked by user limit
	require.False(t, acquired2)

	acquired3, _ := groups.TryAcquire(t.Context(), "user2") // should work
	require.True(t, acquired3)

	acquired4, _ := groups.TryAcquire(t.Context(), "user3") // blocked by global limit
	require.False(t, acquired4)
}

func TestConcurrencyLimit_BasicAcquireRelease(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(2, nil)

	// Should acquire twice
	require.True(t, limit.TryAcquire(t.Context(), "test"))
	require.True(t, limit.TryAcquire(t.Context(), "test"))

	// Third should fail
	require.False(t, limit.TryAcquire(t.Context(), "test"))

	// Release and try again
	limit.Release(t.Context(), "test")
	require.True(t, limit.TryAcquire(t.Context(), "test"))
}

func TestConcurrencyLimit_ZeroLimit(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(0, nil)

	// Should never acquire
	require.False(t, limit.TryAcquire(t.Context(), "test"))
}

func TestConcurrencyLimit_KeyBasedConcurrency(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(1, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Different keys should work independently
	require.True(t, limit.TryAcquire(t.Context(), "key1"))
	require.True(t, limit.TryAcquire(t.Context(), "key2"))

	// Same key should be blocked
	require.False(t, limit.TryAcquire(t.Context(), "key1"))

	// Release key1 and try again
	limit.Release(t.Context(), "key1")
	require.True(t, limit.TryAcquire(t.Context(), "key1"))
}

func TestConcurrencyLimit_NoKeyFunc(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(1, nil)

	// All use same key
	require.True(t, limit.TryAcquire(t.Context(), "data1"))
	require.False(t, limit.TryAcquire(t.Context(), "data2"))

	limit.Release(t.Context(), "data1")
	require.True(t, limit.TryAcquire(t.Context(), "data3"))
}

func TestConcurrencyLimit_HighConcurrency(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(5, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Test concurrent access with different keys
	var acquired int32
	var wg sync.WaitGroup

	// Start 15 goroutines (3 per key for 5 keys)
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			if limit.TryAcquire(t.Context(), key) {
				atomic.AddInt32(&acquired, 1)
				// Hold for a bit then release
				limit.Release(t.Context(), key)
			}
		}(string(rune('a' + i%5)))
	}

	wg.Wait()
	// Should have acquired all 15 since each key can be acquired multiple times sequentially
	require.Equal(t, int32(15), acquired)
}

func TestConcurrencyLimit_OverRelease(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(2, nil)

	// Acquire once
	require.True(t, limit.TryAcquire(t.Context(), "test"))

	// Release once - should work
	limit.Release(t.Context(), "test")

	// Release again - should not hang (even though channel is empty)
	done := make(chan bool, 1)
	go func() {
		limit.Release(t.Context(), "test")
		done <- true
	}()

	select {
	case <-done:
		// Good - didn't hang
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Release hung when trying to release more than acquired")
	}
}

func TestConcurrencyLimit_ReleaseUnknownKey(t *testing.T) {
	limit := waffle.NewConcurrencyLimit(2, func(_ context.Context, data any) string {
		return data.(string)
	})

	// Release on a key that was never used - should not hang
	done := make(chan bool, 1)
	go func() {
		limit.Release(t.Context(), "unknown")
		done <- true
	}()

	select {
	case <-done:
		// Good - didn't hang
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Release hung on unknown key")
	}
}

func BenchmarkConcurrencyLimit(b *testing.B) {
	limit := waffle.NewConcurrencyLimit(100, func(_ context.Context, data any) string {
		return data.(string)
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := string(rune('a' + i%10))
			if limit.TryAcquire(context.Background(), key) {
				limit.Release(context.Background(), key)
			}
			i++
		}
	})
}

func BenchmarkConcurrencyGroups(b *testing.B) {
	groups := waffle.NewConcurrencyGroups()
	groups.AddGlobalLimit(100)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if acquired, release := groups.TryAcquire(context.Background(), "test"); acquired {
				release()
			}
		}
	})
}
