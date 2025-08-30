package waffle

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	actionConcurrencyLimits map[ActionKey]*concurrencyGroups
}

// NewEngine creates a new event engine.
func NewEngine() *Engine {
	return &Engine{
		triggers:                make(map[EventKey]ActionKey),
		actions:                 make(map[ActionKey]Action),
		actionConcurrencyLimits: make(map[ActionKey]*concurrencyGroups),
	}
}

// On registers an action for the given event keys.
func (e *Engine) On(eventKeys ...EventKey) *ActionBuilder {
	return &ActionBuilder{
		engine:            e,
		eventKeys:         eventKeys,
		concurrencyGroups: newConcurrencyGroups(),
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

// ErrBuilderBadParams represents errors that occurred during action builder configuration.
type ErrBuilderBadParams struct {
	Errors []error
}

func (e *ErrBuilderBadParams) Error() string {
	if len(e.Errors) == 0 {
		return "builder errors"
	}

	var errBuilder strings.Builder
	errBuilder.WriteString("builder errors: ")
	for i, err := range e.Errors {
		if i > 0 {
			errBuilder.WriteString(", ")
		}
		errBuilder.WriteString(err.Error())
	}
	return errBuilder.String()
}

// Unwrap returns the underlying errors.
func (e *ErrBuilderBadParams) Unwrap() []error {
	return e.Errors
}

// ActionBuilder builds actions for events.
type ActionBuilder struct {
	engine            *Engine
	eventKeys         []EventKey
	concurrencyGroups *concurrencyGroups
	errors            []error
}

func (ab *ActionBuilder) Concurrency(limit uint) *ActionBuilder {
	if limit == 0 {
		ab.errors = append(ab.errors, fmt.Errorf("Concurrency: limit must be non-negative"))
		return ab
	}

	ab.concurrencyGroups.AddGlobalLimit(uint(limit))

	return ab
}

func (ab *ActionBuilder) ConcurrencyGroup(groupName string, limit uint, keyFunc func(ctx context.Context, data any) string) *ActionBuilder {
	if limit == 0 {
		ab.errors = append(ab.errors, fmt.Errorf("ConcurrencyGroup: limit must be greater than 0"))
		return ab
	}

	if keyFunc == nil {
		ab.errors = append(ab.errors, fmt.Errorf("ConcurrencyGroup: keyFunc must be provided"))
		return ab
	}

	if groupName == "" {
		ab.errors = append(ab.errors, fmt.Errorf("ConcurrencyGroup: groupName must be provided"))
		return ab
	}

	ab.concurrencyGroups.Add(groupName, limit, keyFunc)

	return ab
}

// Do registers the action for all the event keys.
func (ab *ActionBuilder) Do(actionKey ActionKey, action Action) error {
	if actionKey == "" {
		ab.errors = append(ab.errors, fmt.Errorf("Do: actionKey must be provided"))
	}

	if len(ab.eventKeys) == 0 {
		ab.errors = append(ab.errors, fmt.Errorf("Do: eventKeys must be provided"))
	}

	if action == nil {
		ab.errors = append(ab.errors, fmt.Errorf("Do: action must be provided"))
	}

	if len(ab.errors) > 0 {
		return &ErrBuilderBadParams{Errors: ab.errors}
	}

	ab.engine.actions[actionKey] = action

	for _, eventKey := range ab.eventKeys {
		ab.engine.triggers[eventKey] = actionKey
	}

	ab.engine.actionConcurrencyLimits[actionKey] = ab.concurrencyGroups

	return nil
}

type concurrencyGroups struct {
	groups map[string]*concurrencyLimit
	mu     sync.RWMutex
}

func newConcurrencyGroups() *concurrencyGroups {
	return &concurrencyGroups{
		groups: make(map[string]*concurrencyLimit),
	}
}

func (c *concurrencyGroups) AddGlobalLimit(limit uint) {
	c.mu.Lock()
	c.groups[""] = newConcurrencyLimit(limit, nil)
	c.mu.Unlock()
}

func (c *concurrencyGroups) Add(groupName string, limit uint, keyFunc func(ctx context.Context, data any) string) {
	c.mu.Lock()
	c.groups[groupName] = newConcurrencyLimit(limit, keyFunc)
	c.mu.Unlock()
}

func (c *concurrencyGroups) TryAcquire(ctx context.Context, data any) (acquired bool, release func()) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	acquiredGroups := make([]*concurrencyLimit, 0, len(c.groups))
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

// concurrencyLimit is a semaphore that limits the number of concurrent actions.
type concurrencyLimit struct {
	limit      uint
	semaphores map[string]chan struct{}
	keyFunc    func(ctx context.Context, data any) string
}

func newConcurrencyLimit(limit uint, keyFunc func(ctx context.Context, data any) string) *concurrencyLimit {
	return &concurrencyLimit{
		limit:      limit,
		semaphores: make(map[string]chan struct{}),
		keyFunc:    keyFunc,
	}
}

func (c *concurrencyLimit) TryAcquire(ctx context.Context, data any) bool {
	key := c.getKey(ctx, data)

	semaphore, ok := c.semaphores[key]
	if !ok {
		semaphore = make(chan struct{}, c.limit)
		c.semaphores[key] = semaphore
	}

	select {
	case semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *concurrencyLimit) Release(ctx context.Context, data any) {
	key := c.getKey(ctx, data)

	<-c.semaphores[key]
}

func (c *concurrencyLimit) getKey(ctx context.Context, data any) string {
	key := ""

	if c.keyFunc != nil {
		key = c.keyFunc(ctx, data)
	}

	return key
}
