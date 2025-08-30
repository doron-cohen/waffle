package waffle

import (
	"context"
	"fmt"
	"strings"
)

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
	concurrencyGroups *ConcurrencyGroups
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
