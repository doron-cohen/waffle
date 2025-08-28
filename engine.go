package waffle

import (
	"context"
)

// Engine is the main engine for the workflow.
type Engine struct {
	workflows map[string]Workflow
}

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	return &Engine{
		workflows: make(map[string]Workflow),
	}
}

// On creates a new workflow builder for the given event key.
func (e *Engine) On(eventKey string) *WorkflowBuilder {
	return NewWorkflowBuilder(eventKey, e.addWorkflow)
}

// Send sends an event to the engine which will trigger the relevant workflows.
// It returns true if the event was sent, false if no workflows are registered
// for the event.
func (e *Engine) Send(ctx context.Context, eventKey string) bool {
	workflow, ok := e.workflows[eventKey]
	if !ok {
		return false
	}

	go workflow.Run(ctx)

	return true
}

func (e *Engine) addWorkflow(eventKey string, workflow Workflow) error {
	e.workflows[eventKey] = workflow

	return nil
}
