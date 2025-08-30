package waffle

import (
	"context"
	"sync"
)

// ConcurrencyGroups manages multiple concurrency limits.
type ConcurrencyGroups struct {
	groups map[string]*ConcurrencyLimit
	mu     sync.RWMutex
}

// NewConcurrencyGroups creates a new ConcurrencyGroups instance.
func NewConcurrencyGroups() *ConcurrencyGroups {
	return &ConcurrencyGroups{
		groups: make(map[string]*ConcurrencyLimit),
	}
}

// AddGlobalLimit adds a global concurrency limit.
func (c *ConcurrencyGroups) AddGlobalLimit(limit uint) {
	c.mu.Lock()
	c.groups[""] = NewConcurrencyLimit(limit, nil)
	c.mu.Unlock()
}

// Add adds a named concurrency group with a limit and key function.
func (c *ConcurrencyGroups) Add(groupName string, limit uint, keyFunc func(ctx context.Context, data any) string) {
	c.mu.Lock()
	c.groups[groupName] = NewConcurrencyLimit(limit, keyFunc)
	c.mu.Unlock()
}

// TryAcquire attempts to acquire all concurrency limits.
func (c *ConcurrencyGroups) TryAcquire(ctx context.Context, data any) (acquired bool, release func()) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	acquiredGroups := make([]*ConcurrencyLimit, 0, len(c.groups))
	canRun := true
	for _, group := range c.groups {
		if !group.TryAcquire(ctx, data) {
			canRun = false
			break
		}

		acquiredGroups = append(acquiredGroups, group)
	}

	releaseFunc := func() {
		for _, group := range acquiredGroups {
			group.Release(ctx, data)
		}
	}

	if canRun {
		return true, releaseFunc
	}

	releaseFunc()
	return false, nil
}

// ConcurrencyLimit is a semaphore that limits the number of concurrent actions.
type ConcurrencyLimit struct {
	limit      uint
	semaphores map[string]chan struct{}
	keyFunc    func(ctx context.Context, data any) string
	mu         sync.RWMutex
}

// NewConcurrencyLimit creates a new ConcurrencyLimit with the specified limit and key function.
func NewConcurrencyLimit(limit uint, keyFunc func(ctx context.Context, data any) string) *ConcurrencyLimit {
	return &ConcurrencyLimit{
		limit:      limit,
		semaphores: make(map[string]chan struct{}),
		keyFunc:    keyFunc,
	}
}

// TryAcquire attempts to acquire a slot in the concurrency limit.
func (c *ConcurrencyLimit) TryAcquire(ctx context.Context, data any) bool {
	key := c.getKey(ctx, data)

	c.mu.Lock()
	semaphore, ok := c.semaphores[key]
	if !ok {
		semaphore = make(chan struct{}, c.limit)
		c.semaphores[key] = semaphore
	}
	c.mu.Unlock()

	select {
	case semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a slot in the concurrency limit.
func (c *ConcurrencyLimit) Release(ctx context.Context, data any) {
	key := c.getKey(ctx, data)

	c.mu.RLock()
	semaphore, ok := c.semaphores[key]
	c.mu.RUnlock()

	if ok {
		// Non-blocking release - if channel is empty, don't block
		select {
		case <-semaphore:
			// Successfully released
		default:
			// Channel is empty (over-release), do nothing
		}
	}
}

func (c *ConcurrencyLimit) getKey(ctx context.Context, data any) string {
	key := ""

	if c.keyFunc != nil {
		key = c.keyFunc(ctx, data)
	}

	return key
}
