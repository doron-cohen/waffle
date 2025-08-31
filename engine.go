package waffle

import (
	"context"
	"strings"
)

type (
	// EventKey is a unique identifier for an event.
	EventKey string

	// ActionKey is a unique identifier for an action.
	ActionKey string

	// Action is a function that will be executed when the event is triggered.
	Action func(ctx context.Context, data any) error
)

// ActionConfiguration represents the configuration of an action.
type ActionConfiguration struct {
	EventKeys         []EventKey
	ConcurrencyGroups *ConcurrencyGroups
	ActionKey         ActionKey
	Action            Action
}

// OperationLogger logs internal engine operations
type OperationLogger interface {
	LogOperation(ctx context.Context, event string, metadata map[string]string)
}

// Engine maps events to actions and executes them.
type Engine struct {
	// triggers maps event keys to their corresponding actions
	triggers map[EventKey][]ActionKey
	// actions maps action keys to their corresponding actions
	actions map[ActionKey]Action
	// actionConcurrencyLimits maps action keys to their concurrency configuration
	actionConcurrencyLimits map[ActionKey]*ConcurrencyGroups
	// operationLogger logs internal engine operations
	operationLogger OperationLogger
}

// NewEngine creates a new event engine.
func NewEngine(operationLogger OperationLogger) *Engine {
	return &Engine{
		triggers:                make(map[EventKey][]ActionKey),
		actions:                 make(map[ActionKey]Action),
		actionConcurrencyLimits: make(map[ActionKey]*ConcurrencyGroups),
		operationLogger:         operationLogger,
	}
}

// logOperation logs an internal engine operation if a logger is set
func (e *Engine) logOperation(ctx context.Context, event string, metadata map[string]string) {
	if e.operationLogger != nil {
		e.operationLogger.LogOperation(ctx, event, metadata)
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
	actionKeys, ok := e.triggers[eventKey]
	if !ok {
		return false
	}

	// Log event received for non-internal events
	if !strings.HasPrefix(string(eventKey), "waffle.") {
		e.logOperation(ctx, "waffle.event.received", map[string]string{
			"eventKey": string(eventKey),
		})
	}

	for _, actionKey := range actionKeys {
		e.spawnAction(ctx, actionKey, data, eventKey)
	}

	return true
}

// AddActionConfiguration adds an action configuration to the engine.
func (e *Engine) AddActionConfiguration(configuration ActionConfiguration) {
	// TODO: move validations here
	e.actions[configuration.ActionKey] = configuration.Action

	for _, eventKey := range configuration.EventKeys {
		e.triggers[eventKey] = append(e.triggers[eventKey], configuration.ActionKey)
	}

	e.actionConcurrencyLimits[configuration.ActionKey] = configuration.ConcurrencyGroups
}

func (e *Engine) spawnAction(ctx context.Context, actionKey ActionKey, data any, eventKey EventKey) {
	action, ok := e.actions[actionKey]
	if !ok {
		// Log action spawn failed
		e.logOperation(ctx, "waffle.action.spawn_failed", map[string]string{
			"actionKey": string(actionKey),
			"eventKey":  string(eventKey),
		})
		return
	}

	// Log action spawned
	e.logOperation(ctx, "waffle.action.spawned", map[string]string{
		"actionKey": string(actionKey),
		"eventKey":  string(eventKey),
	})

	acquired, release := true, func() {}
	groups := e.actionConcurrencyLimits[actionKey]
	if len(groups.groups) > 0 {
		acquired, release = groups.TryAcquire(ctx, data)
		if acquired {
			// Log concurrency acquire success
			e.logOperation(ctx, "waffle.concurrency.acquire_success", map[string]string{
				"actionKey": string(actionKey),
			})
		} else {
			// Log concurrency acquire failed
			e.logOperation(ctx, "waffle.concurrency.acquire_failed", map[string]string{
				"actionKey": string(actionKey),
			})
			return
		}
	}

	// Create release function that logs released event
	originalRelease := release
	release = func() {
		originalRelease()
		if len(groups.groups) > 0 {
			// Log concurrency released
			e.logOperation(ctx, "waffle.concurrency.released", map[string]string{
				"actionKey": string(actionKey),
			})
		}
	}

	go func(_release func()) {
		defer _release()
		// Log action started
		e.logOperation(ctx, "waffle.action.started", map[string]string{
			"actionKey": string(actionKey),
			"eventKey":  string(eventKey),
		})
		// TODO: handle errors
		_ = action(ctx, data)
	}(release)
}
