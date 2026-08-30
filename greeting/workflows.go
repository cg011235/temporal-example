package greeting

import (
	"time" // Used only to set activity timeouts; workflows must not call time.Now().

	"go.temporal.io/sdk/workflow" // Temporal SDK types for writing deterministic workflows.
)

// TaskQueue is the named queue that workers poll and that starters use when
// they start a workflow. The worker and starter must agree on this string.
const TaskQueue = "greeting-task-queue"

// GreetingWorkflow is a workflow: Temporal's durable, replayable orchestration.
//
// Workflows must be deterministic. They should not do I/O themselves; they
// schedule activities (and other Temporal primitives) so the server can record
// each step and resume after crashes or restarts.
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	// workflow.GetLogger is the replay-safe logger. Do not use the standard log package here.
	workflow.GetLogger(ctx).Info("GreetingWorkflow started", "name", name)

	// ActivityOptions tell Temporal how to run activities scheduled from this context.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// StartToCloseTimeout is the maximum time one activity attempt may run.
		// Without a timeout, Temporal will not schedule the activity.
		StartToCloseTimeout: 10 * time.Second,
	})

	// result will receive the activity's return value once it completes.
	var result string
	// ExecuteActivity schedules ComposeGreeting on the task queue. The activity
	// does not run in this process; a worker picks it up. .Get() waits for the
	// recorded result (or a failure) and unmarshals it into result.
	err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &result)
	if err != nil {
		// Propagate the activity failure so the workflow run is marked failed.
		return "", err
	}

	workflow.GetLogger(ctx).Info("GreetingWorkflow completed", "result", result)
	// Returning a nil error marks the workflow as completed successfully.
	return result, nil
}
