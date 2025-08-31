package waffle

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// OperationLog represents a single logged operation
type OperationLog struct {
	Event    string
	Metadata map[string]string
}

// TestOperationLogger captures logged operations for testing
type TestOperationLogger struct {
	logs []OperationLog
}

// NewTestOperationLogger creates a new test operation logger
func NewTestOperationLogger() *TestOperationLogger {
	return &TestOperationLogger{
		logs: make([]OperationLog, 0),
	}
}

// LogOperation implements the OperationLogger interface
func (l *TestOperationLogger) LogOperation(ctx context.Context, event string, metadata map[string]string) {
	l.logs = append(l.logs, OperationLog{
		Event:    event,
		Metadata: metadata,
	})
}

// AssertEventLogged asserts that a specific event was logged
func (l *TestOperationLogger) AssertEventLogged(t *testing.T, event string) {
	t.Helper()
	for _, log := range l.logs {
		if log.Event == event {
			return
		}
	}
	t.Errorf("Expected event '%s' to be logged, but it wasn't. Logged events: %v", event, l.getEventNames())
}

// AssertEventLoggedWithMetadata asserts that a specific event was logged with specific metadata
func (l *TestOperationLogger) AssertEventLoggedWithMetadata(t *testing.T, event string, expectedMetadata map[string]string) {
	t.Helper()
	for _, log := range l.logs {
		if log.Event == event {
			for key, expectedValue := range expectedMetadata {
				actualValue, exists := log.Metadata[key]
				if !exists {
					t.Errorf("Expected metadata key '%s' not found in event '%s'. Metadata: %v", key, event, log.Metadata)
					return
				}
				if actualValue != expectedValue {
					t.Errorf("Expected metadata key '%s' to have value '%s', but got '%s' in event '%s'", key, expectedValue, actualValue, event)
					return
				}
			}
			return
		}
	}
	t.Errorf("Expected event '%s' with metadata %v to be logged, but it wasn't. Logged events: %v", event, expectedMetadata, l.getEventNames())
}

// AssertEventNotLogged asserts that a specific event was NOT logged
func (l *TestOperationLogger) AssertEventNotLogged(t *testing.T, event string) {
	t.Helper()
	for _, log := range l.logs {
		if log.Event == event {
			t.Errorf("Expected event '%s' to NOT be logged, but it was found in logs", event)
			return
		}
	}
}

// AssertEventLoggedTimes asserts that a specific event was logged exactly n times
func (l *TestOperationLogger) AssertEventLoggedTimes(t *testing.T, event string, expectedCount int) {
	t.Helper()
	count := 0
	for _, log := range l.logs {
		if log.Event == event {
			count++
		}
	}
	if count != expectedCount {
		t.Errorf("Expected event '%s' to be logged %d times, but it was logged %d times", event, expectedCount, count)
	}
}

// AssertNoEventsLogged asserts that no events were logged
func (l *TestOperationLogger) AssertNoEventsLogged(t *testing.T) {
	t.Helper()
	if len(l.logs) > 0 {
		t.Errorf("Expected no events to be logged, but %d events were logged: %v", len(l.logs), l.getEventNames())
	}
}

// Clear clears all logged events
func (l *TestOperationLogger) Clear() {
	l.logs = make([]OperationLog, 0)
}

// GetLogs returns all logged operations
func (l *TestOperationLogger) GetLogs() []OperationLog {
	return l.logs
}

// getEventNames returns a slice of all logged event names for error messages
func (l *TestOperationLogger) getEventNames() []string {
	events := make([]string, len(l.logs))
	for i, log := range l.logs {
		events[i] = log.Event
	}
	return events
}

// String returns a string representation of all logged events
func (l *TestOperationLogger) String() string {
	if len(l.logs) == 0 {
		return "No events logged"
	}

	var sb strings.Builder
	sb.WriteString("Logged events:\n")
	for _, log := range l.logs {
		sb.WriteString(fmt.Sprintf("  - %s: %v\n", log.Event, log.Metadata))
	}
	return sb.String()
}
