package waffle

import (
	"context"
)

type (
	// EventKey is a unique identifier for an event.
	EventKey string

	// ActionKey is a unique identifier for an action.
	ActionKey string

	// Action is a function that will be executed when the event is triggered.
	Action func(ctx context.Context, data any) error
)

// Engine maps events to actions and executes them.
type Engine struct {
	// triggers maps event keys to their corresponding actions
	triggers map[EventKey]ActionKey
	// actions maps action keys to their corresponding actions
	actions map[ActionKey]Action
	// actionConcurrencyLimits maps action keys to their concurrency configuration
	actionConcurrencyLimits map[ActionKey]*ConcurrencyGroups
}

// NewEngine creates a new event engine.
func NewEngine() *Engine {
	return &Engine{
		triggers:                make(map[EventKey]ActionKey),
		actions:                 make(map[ActionKey]Action),
		actionConcurrencyLimits: make(map[ActionKey]*ConcurrencyGroups),
	}
}

// On registers an action for the given event keys.
func (e *Engine) On(eventKeys ...EventKey) *ActionBuilder {
	return &ActionBuilder{
		engine:            e,
		eventKeys:         eventKeys,
		concurrencyGroups: NewConcurrencyGroups(),
		errors:            make([]error, 0),
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

	acquired, release := true, func() {}
	groups := e.actionConcurrencyLimits[actionKey]
	if len(groups.groups) > 0 {
		acquired, release = groups.TryAcquire(ctx, data)
	}

	if !acquired {
		return
	}

	go func(_release func()) {
		defer _release()
		// TODO: handle errors
		_ = action(ctx, data)
	}(release)
}
