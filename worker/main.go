// Command worker polls a Temporal task queue and executes registered workflows and activities.
package main

import (
	"log"                      // Standard library logger for process-level messages.
	"temporal-example/greeting" // Local package that defines the workflow, activity, and task queue name.

	"go.temporal.io/sdk/client" // Client used to connect to the Temporal server.
	"go.temporal.io/sdk/worker" // Worker that polls the task queue and runs registered functions.
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

	// worker.New binds this process to a task queue. Only workflows started on
	// greeting.TaskQueue will be offered to this worker.
	w := worker.New(c, greeting.TaskQueue, worker.Options{})

	// RegisterWorkflow tells Temporal which function implements the workflow type.
	w.RegisterWorkflow(greeting.GreetingWorkflow)
	// RegisterActivity tells Temporal which function implements the activity type.
	w.RegisterActivity(greeting.ComposeGreeting)

	log.Printf("Worker started running on %q...", greeting.TaskQueue)
	// Run starts polling. InterruptCh() stops the worker on SIGINT/SIGTERM
	// (Ctrl+C). This call blocks until the worker stops.
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
}
