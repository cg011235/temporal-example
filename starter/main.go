// Command starter starts a GreetingWorkflow and waits for its result.
package main

import (
	"context"                    // Used to cancel or time out the start and result-wait calls.
	"log"                        // Standard library logger for process-level messages.
	"temporal-example/greeting"   // Local package that defines the workflow function and task queue name.

	"go.temporal.io/sdk/client" // Client used to talk to the Temporal server (not to run work itself).
)

func main() {
	// Dial opens a gRPC connection to the Temporal server.
	// Empty Options{} uses the default address localhost:7233.
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// Close the connection when main returns so the process shuts down cleanly.
	defer c.Close()

	// ID uniquely identifies this workflow. Reusing the same ID while a run is
	// still open will attach to that run instead of starting a duplicate.
	workflowID := "greeting-workflow"
	// workflowInput is the name passed as the workflow's first argument.
	workflowInput := "Temporal"

	// ExecuteWorkflow asks the Temporal server to start GreetingWorkflow.
	// The worker process (not this starter) will actually run the code.
	workflowRun, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,       // Client-chosen identifier for this workflow.
		TaskQueue: greeting.TaskQueue, // Must match the queue the worker is polling.
	}, greeting.GreetingWorkflow, workflowInput) // Function to run, then its arguments.
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	// GetID is the workflow ID we supplied above.
	log.Printf("Workflow started: %s", workflowRun.GetID())
	// GetRunID is Temporal's unique ID for this particular execution attempt.
	log.Printf("Workflow run: %s", workflowRun.GetRunID())

	// result will receive the workflow's return value once it completes.
	var result string
	// Get blocks until the workflow finishes and unmarshals the result.
	err = workflowRun.Get(context.Background(), &result)
	if err != nil {
		log.Fatalf("Failed to get workflow result: %v", err)
	}

	log.Printf("Workflow result: %s", result)
}
