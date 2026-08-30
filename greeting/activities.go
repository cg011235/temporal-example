// Package greeting holds the Temporal workflow and activity implementations.
package greeting

import (
	"context" // Go's standard context type; activities use this, not workflow.Context.
	"fmt"     // Used to format the greeting string returned by the activity.

	"go.temporal.io/sdk/activity" // Temporal SDK helpers for activity logging and metadata.
)

// ComposeGreeting is an activity: a short, retryable function that performs real work.
//
// Unlike a workflow, an activity is allowed to do non-deterministic things such as
// HTTP requests, database calls, or reading the current time. Temporal records
// the activity's result so the workflow can replay without running it again.
func ComposeGreeting(ctx context.Context, name string) (string, error) {
	// Real side effects belong here: HTTP requests, database calls, etc.
	// activity.GetLogger(ctx) returns a logger bound to this activity execution.
	activity.GetLogger(ctx).Info("Composing greeting", "name", name)
	// Build and return the greeting. A nil error means the activity succeeded.
	return fmt.Sprintf("Hello, %s!", name), nil
}
