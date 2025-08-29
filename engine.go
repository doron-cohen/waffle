package waffle

import (
	"context"
)

type (
	EventKey string
	Action   func(ctx context.Context, data any) error
)

// Engine maps events to actions and executes them.
type Engine struct {
	// actions maps event keys to their corresponding actions
	actions map[EventKey]Action
}

// NewEngine creates a new event engine.
func NewEngine() *Engine {
	return &Engine{
		actions: make(map[EventKey]Action),
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
	action, ok := e.actions[eventKey]
	if !ok {
		return false
	}

	go action(ctx, data)
	return true
}

// ActionBuilder builds actions for events.
type ActionBuilder struct {
	engine    *Engine
	eventKeys []EventKey
}

// Do registers the action for all the event keys.
func (ab *ActionBuilder) Do(action Action) {
	for _, eventKey := range ab.eventKeys {
		ab.engine.actions[eventKey] = action
	}
}
