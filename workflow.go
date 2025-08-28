package waffle

import "context"

// WorkflowBuilder is a builder for a workflow.
type WorkflowBuilder struct {
	eventKey        string
	workflow        Workflow
	addWorkflowFunc func(eventKey string, workflow Workflow) error
}

func NewWorkflowBuilder(eventKey string, addWorkflow func(eventKey string, workflow Workflow) error) *WorkflowBuilder {
	return &WorkflowBuilder{
		eventKey:        eventKey,
		workflow:        Workflow{},
		addWorkflowFunc: addWorkflow,
	}
}

func (w *WorkflowBuilder) Do(f func(ctx context.Context, data any) error) *WorkflowBuilder {
	w.workflow.action = f

	return w
}

func (w *WorkflowBuilder) Build() error {
	w.addWorkflowFunc(w.eventKey, w.workflow)

	return nil
}

type Workflow struct {
	action func(ctx context.Context, data any) error
}

// Run runs the workflow.
func (w *Workflow) Run(ctx context.Context, data any) error {
	return w.action(ctx, data)
}
