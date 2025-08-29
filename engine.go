package waffle

import (
	"context"
	"sync"
)

type (
	EventKey  string
	ActionKey string
	Action    func(ctx context.Context, data any) error
)

// Engine maps events to actions and executes them.
type Engine struct {
	// triggers maps event keys to their corresponding actions
	triggers map[EventKey]ActionKey
	// actions maps action keys to their corresponding actions
	actions map[ActionKey]Action
	// concurrencyGroups maps action keys to their concurrency configuration
	concurrencyGroups   map[ActionKey]*concurrencyLimit
	concurrencyGroupsMu sync.RWMutex
}

// NewEngine creates a new event engine.
func NewEngine() *Engine {
	return &Engine{
		triggers:          make(map[EventKey]ActionKey),
		actions:           make(map[ActionKey]Action),
		concurrencyGroups: make(map[ActionKey]*concurrencyLimit),
	}
}

// On registers an action for the given event keys.
func (e *Engine) On(eventKeys ...EventKey) *ActionBuilder {
	return &ActionBuilder{
		engine:    e,
		eventKeys: eventKeys,
	}
}

// Send sends an event to the engine which will trigger the registered action.
// It returns true if the event was sent, false if no action is registered for the event.
func (e *Engine) Send(ctx context.Context, eventKey EventKey, data any) bool {
	actionKey, ok := e.triggers[eventKey]
	if !ok {
		return false
	}

	e.spawnAction(ctx, actionKey, data)

	return true
}

func (e *Engine) spawnAction(ctx context.Context, actionKey ActionKey, data any) {
	action, ok := e.actions[actionKey]
	if !ok {
		return
	}

	e.concurrencyGroupsMu.RLock()
	limit := e.concurrencyGroups[actionKey]
	e.concurrencyGroupsMu.RUnlock()

	if limit != nil && !limit.TryAcquire() {
		return
	}

	go func(limit *concurrencyLimit) {
		if limit != nil {
			defer limit.Release()
		}

		// TODO: handle errors
		_ = action(ctx, data)
	}(limit)
}

// ActionBuilder builds actions for events.
type ActionBuilder struct {
	engine           *Engine
	eventKeys        []EventKey
	concurrencyLimit int
}

func (ab *ActionBuilder) Concurrency(limit int) *ActionBuilder {
	ab.concurrencyLimit = limit

	return ab
}

// Do registers the action for all the event keys.
func (ab *ActionBuilder) Do(actionKey ActionKey, action Action) {
	ab.engine.actions[actionKey] = action

	for _, eventKey := range ab.eventKeys {
		ab.engine.triggers[eventKey] = actionKey
	}

	if ab.concurrencyLimit > 0 {
		ab.engine.concurrencyGroupsMu.Lock()
		ab.engine.concurrencyGroups[actionKey] = newConcurrencyLimit(ab.concurrencyLimit)
		ab.engine.concurrencyGroupsMu.Unlock()
	}
}

// concurrencyLimit is a semaphore that limits the number of concurrent actions.
type concurrencyLimit struct {
	limit     int
	semaphore chan struct{}
}

func newConcurrencyLimit(limit int) *concurrencyLimit {
	return &concurrencyLimit{
		limit:     limit,
		semaphore: make(chan struct{}, limit),
	}
}

func (c *concurrencyLimit) TryAcquire() bool {
	select {
	case c.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *concurrencyLimit) Release() {
	<-c.semaphore
}
