# GreetingWorkflow — Detailed Interaction Diagrams

This document explains, in depth, everything that happens when this repository's
example runs: how the **Starter**, the **Worker**, and the **Temporal Cluster**
talk to each other over gRPC, what the Temporal Server persists at every step,
and how the Go SDK executes deterministic workflow code under the hood.

## 0. Scope & assumptions

These diagrams describe one concrete, reproducible run of this exact codebase:

- One Temporal Server, started via `docker-compose.yml` as `temporal server start-dev --ip 0.0.0.0 --db-filename /home/temporal/temporal.db`, exposing gRPC on `7233` and the Web UI on `8233` (Web UI port = gRPC port + 1000, the dev-server default).
- Because `--db-filename` is set, workflow state is persisted to a SQLite file on the `temporal-data` volume instead of the dev server's normal in-memory mode — history survives `docker compose restart`, but not volume deletion.
- The `default` Namespace (auto-created by `start-dev`), since neither `worker/main.go` nor `starter/main.go` overrides `client.Options.Namespace`.
- One Worker process (`worker/main.go`) polling Task Queue `greeting-task-queue` (`greeting.TaskQueue`), with default `worker.Options{}` — meaning 2 concurrent Workflow Task poller goroutines and 2 concurrent Activity Task poller goroutines (Go SDK default `defaultConcurrentPollRoutineSize = 2`), and Sticky Execution enabled (the SDK's default caching optimization).
- One Starter invocation (`starter/main.go`) starting Workflow ID `greeting-workflow` with input `"Temporal"`, then blocking on the result.
- A single activity call that succeeds on the first attempt — no retries, timeouts, signals, or queries are exercised by the happy path. Failure/replay paths are covered separately in the [Appendix](#9-appendix-paths-not-exercised-by-this-run) for completeness.

### Notation used below

| Symbol | Meaning |
|---|---|
| `->>` | Synchronous request (gRPC call) |
| `-->>` | Response / return |
| `Note` | Internal detail, commentary, or non-network step |
| Colored `rect` band | Groups messages into one logical phase |
| `par ... and ... end` | Two branches that genuinely happen concurrently, not sequentially |

---

## 1. Components inventory

| Component | What it is | Defined in |
|---|---|---|
| **Starter process** | Short-lived CLI that starts one workflow and waits for its result | `starter/main.go` |
| **Worker process** | Long-lived process that polls Temporal and executes workflow/activity code | `worker/main.go` |
| **`GreetingWorkflow`** | The workflow definition (orchestration logic) | `greeting/workflows.go` |
| **`ComposeGreeting`** | The activity definition (does the actual "work") | `greeting/activities.go` |
| **`TaskQueue` constant** | `"greeting-task-queue"` — shared contract between Worker and Starter | `greeting/workflows.go` |
| **Temporal Go SDK client** | gRPC client wrapper (`client.Dial`) used by both processes | `go.temporal.io/sdk/client` |
| **Temporal Go SDK worker runtime** | Pollers, deterministic dispatcher, sticky cache, activity pool | `go.temporal.io/sdk/worker` (inside the Worker process) |
| **Frontend Service** | Single gRPC entry point (`WorkflowService` API) for the whole cluster | Inside the `temporal` container |
| **History Service** | Owns per-execution mutable state & the append-only Event History | Inside the `temporal` container |
| **Matching Service** | Buffers tasks per Task Queue and matches them to long-polling workers | Inside the `temporal` container |
| **Persistence store** | SQLite file `temporal.db` on the `temporal-data` volume | Inside the `temporal` container |
| **Web UI** | Read-only observability UI over the Frontend API | Inside the `temporal` container, port `8233` |

> In production, Frontend/History/Matching (and visibility, e.g. Elasticsearch) are usually independently-scaled services/containers. `temporal server start-dev` runs all of them **in a single process** for local development — `docker-compose.yml` in this repo defines only **one** container/service (`temporal`), not four.

---

## 2. Deployment diagram

![Deployment diagram](diagrams/01-deployment.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
flowchart LR
    subgraph Host["Local machine"]
        StarterProc["go run starter/main.go\n(Starter process)"]
        WorkerProc["go run worker/main.go\n(Worker process, long-running)"]
        Browser["Your browser"]

        subgraph Docker["docker-compose.yml"]
            Container["Container: temporal\nimage: temporalio/temporal:latest\ncmd: server start-dev --ip 0.0.0.0\n--db-filename /home/temporal/temporal.db"]
        end
        Vol[("Named volume\ntemporal-data")]
    end

    StarterProc -- "localhost:7233 (gRPC)" --> Container
    WorkerProc -- "localhost:7233 (gRPC)" --> Container
    Browser -- "localhost:8233 (HTTP)" --> Container
    Container -- "mounted at /home/temporal" --- Vol
```

</details>

---

## 3. Component / architecture diagram

![Component and architecture diagram](diagrams/02-component-architecture.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
flowchart TB
    subgraph SP["Starter process — starter/main.go"]
        SC["SDK Client\nclient.Dial + ExecuteWorkflow + workflowRun.Get"]
    end

    subgraph WP["Worker process — worker/main.go"]
        direction TB
        REG["Type Registry\nGreetingWorkflow, ComposeGreeting"]
        WFPOLL["Workflow Task Poller(s)\nPollWorkflowTaskQueue\n(2 goroutines by default)"]
        APOLL["Activity Task Poller(s)\nPollActivityTaskQueue\n(2 goroutines by default)"]
        ENG["Deterministic Workflow Engine\ncoroutine dispatcher + Sticky Cache"]
        POOL["Activity Execution Pool\n(plain goroutines, real I/O allowed)"]
        REG --> ENG
        REG --> POOL
        WFPOLL --> ENG
        APOLL --> POOL
    end

    subgraph TC["Temporal Cluster — single container 'temporal' (start-dev)"]
        FE["Frontend Service\ngRPC :7233 — WorkflowService API"]
        HIST["History Service\nmutable state + Event History\nnamespace 'default'"]
        MATCH["Matching Service\nTask Queue 'greeting-task-queue'\n(workflow / activity / sticky partitions)"]
        DB[("Persistence Store\nSQLite file temporal.db\nvolume temporal-data")]
        UI["Web UI\n:8233"]
    end

    SC == "StartWorkflowExecution\nGetWorkflowExecutionHistory" ==> FE
    WFPOLL == "long-poll gRPC" ==> FE
    APOLL == "long-poll gRPC" ==> FE
    ENG == "RespondWorkflowTaskCompleted" ==> FE
    POOL == "RespondActivityTaskCompleted" ==> FE

    FE --> HIST
    FE --> MATCH
    HIST <==> DB
    HIST --> MATCH
    MATCH --> HIST

    UI --> FE
    Person(["You, via browser"]) -.-> UI
```

</details>

---

## 4. Task Queue anatomy

A single string constant, `greeting-task-queue`, actually names **three independent
queues** inside the Matching Service. This is easy to miss when reading the code,
since both pollers in `worker/main.go` are constructed from the same `worker.New(c,
greeting.TaskQueue, worker.Options{})` call.

![Task queue anatomy diagram](diagrams/03-task-queue-anatomy.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
flowchart LR
    subgraph TQ["Matching Service — logical queues named 'greeting-task-queue'"]
        WFQ["Workflow Task partition\n(normal)"]
        ACTQ["Activity Task partition"]
        STICKY["Sticky Workflow Task partition\nephemeral, unique per Worker process,\nauto-created after the first\nRespondWorkflowTaskCompleted"]
    end
    WFPOLL["Worker: Workflow Task Poller"] --> WFQ
    WFPOLL --> STICKY
    APOLL["Worker: Activity Task Poller"] --> ACTQ
```

</details>

- The **normal** workflow partition is used only for a brand-new run's very first Workflow Task (nothing is cached yet).
- The **sticky** partition is a private queue keyed by this Worker's identity, created the moment the Worker first responds to a Workflow Task for a given Run. Temporal prefers routing follow-up Workflow Tasks for that Run back to the same Worker here, because that Worker already has the coroutine state cached in memory — avoiding a full history replay.
- The **activity** partition is completely independent and can be served by any Worker polling the same Task Queue name.

---

## 5. End-to-end sequence diagram (full lifecycle)

This is the complete, chronological interaction for one run: `starter` starts
`GreetingWorkflow("Temporal")`, the workflow schedules the `ComposeGreeting`
activity, the activity runs, and the workflow completes with `"Hello, Temporal!"`.

![End-to-end sequence diagram](diagrams/04-end-to-end-sequence.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
sequenceDiagram
    autonumber
    participant Starter as Starter Process
    participant FE as Frontend Service (7233)
    participant Matching as Matching Service
    participant History as History Service
    participant DB as Persistence (SQLite)
    participant Worker as Worker Process

    rect rgb(240, 248, 255)
    Note over Worker,Matching: Phase 0 — Worker boots and starts long-polling (independent of when Starter runs)
    Worker->>FE: Dial gRPC (localhost:7233)
    Worker->>Matching: PollWorkflowTaskQueue(greeting-task-queue) [long poll, blocks up to ~60s]
    Worker->>Matching: PollActivityTaskQueue(greeting-task-queue) [long poll, blocks up to ~60s]
    end

    rect rgb(255, 250, 240)
    Note over Starter,Worker: Phase 1 — Starter starts the workflow
    Starter->>FE: Dial gRPC (localhost:7233)
    Starter->>FE: StartWorkflowExecution(Id=greeting-workflow, Type=GreetingWorkflow, Input="Temporal", TaskQueue=greeting-task-queue)
    FE->>History: StartWorkflowExecution
    History->>History: create mutable state, generate RunId (uuid)
    History->>DB: append event 1 WorkflowExecutionStarted
    History->>DB: append event 2 WorkflowTaskScheduled
    par respond to Starter immediately
        History-->>FE: RunId
        FE-->>Starter: StartWorkflowExecutionResponse(RunId)
        Starter->>Starter: log "Workflow started" / "Workflow run"
        Starter->>FE: GetWorkflowExecutionHistory(waitForNewEvent=true)
        FE->>History: long poll, waiting for a close event
        Note over Starter: blocked inside workflowRun.Get(ctx, &result)
    and dispatch the first Workflow Task
        History->>Matching: AddWorkflowTask(greeting-task-queue, normal partition)
        Matching->>History: RecordWorkflowTaskStarted (sync match vs. Worker's waiting poll)
        History->>DB: append event 3 WorkflowTaskStarted
        History-->>Matching: task info + history[1..3]
        Matching-->>Worker: PollWorkflowTaskQueue response (TaskToken A)
    end
    end

    rect rgb(240, 255, 240)
    Note over Worker,Matching: Phase 2 — Worker runs GreetingWorkflow to its first blocking point
    Worker->>Worker: execute GreetingWorkflow(ctx, "Temporal") -> calls ExecuteActivity(ComposeGreeting).Get() and blocks
    Worker->>FE: RespondWorkflowTaskCompleted(TaskToken A, command=ScheduleActivityTask{ComposeGreeting, "Temporal", StartToCloseTimeout=10s})
    FE->>History: RespondWorkflowTaskCompleted
    History->>DB: append event 4 WorkflowTaskCompleted
    History->>DB: append event 5 ActivityTaskScheduled
    History->>Matching: AddActivityTask(greeting-task-queue)
    Matching->>History: RecordActivityTaskStarted (sync match vs. Worker's waiting poll)
    History->>DB: append event 6 ActivityTaskStarted
    History-->>Matching: activity task info
    Matching-->>Worker: PollActivityTaskQueue response (TaskToken B, ComposeGreeting, Input="Temporal")
    end

    rect rgb(255, 240, 245)
    Note over Worker,Matching: Phase 3 — Worker executes the ComposeGreeting activity
    Worker->>Worker: run ComposeGreeting(ctx, "Temporal") -> "Hello, Temporal!"
    Worker->>FE: RespondActivityTaskCompleted(TaskToken B, result="Hello, Temporal!")
    FE->>History: RespondActivityTaskCompleted
    History->>DB: append event 7 ActivityTaskCompleted
    History->>DB: append event 8 WorkflowTaskScheduled
    History->>Matching: AddWorkflowTask(sticky partition for this Worker)
    Matching->>History: RecordWorkflowTaskStarted (sync match, same Worker, sticky cache hit)
    History->>DB: append event 9 WorkflowTaskStarted
    History-->>Matching: task info + incremental history[7..9]
    Matching-->>Worker: PollWorkflowTaskQueue response (TaskToken C)
    end

    rect rgb(245, 245, 255)
    Note over Worker,History: Phase 4 — Worker resumes the cached workflow and completes it
    Worker->>Worker: resume cached coroutine, inject activity result, GreetingWorkflow returns ("Hello, Temporal!", nil)
    Worker->>FE: RespondWorkflowTaskCompleted(TaskToken C, command=CompleteWorkflowExecution{result="Hello, Temporal!"})
    FE->>History: RespondWorkflowTaskCompleted
    History->>DB: append event 10 WorkflowTaskCompleted
    History->>DB: append event 11 WorkflowExecutionCompleted
    History->>History: mark execution Closed / Completed, update visibility record
    end

    rect rgb(255, 255, 224)
    Note over Starter,History: Phase 5 — Starter's blocked Get() unblocks
    History-->>FE: release long poll (history now has a close event)
    FE-->>Starter: GetWorkflowExecutionHistory response containing WorkflowExecutionCompleted
    Starter->>Starter: decode payload -> result = "Hello, Temporal!"
    Note over Starter: log "Workflow result: Hello, Temporal!"
    end
```

</details>

**Why does the workflow need *two* Workflow Tasks?** `workflow.ExecuteActivity(...)`
is asynchronous — it returns a `Future` immediately. The first Workflow Task can
only run `GreetingWorkflow` as far as the `.Get(ctx, &result)` call, at which
point the coroutine yields (blocks) because the activity hasn't run yet. Temporal
must persist that "waiting" state, run the activity elsewhere, and only then
deliver a *second* Workflow Task that resumes the coroutine with the activity's
result and lets it reach `return result, nil`. Every side-effect-producing call
in a workflow follows this schedule → suspend → resume pattern.

---

## 6. Inside the Worker: SDK execution mechanics

The end-to-end diagram treats "Worker" as mostly a black box for its two
`Worker->>Worker` steps. This diagram opens that box up: it shows the SDK-internal
handoff between pollers, the deterministic dispatcher, the sticky cache, and the
plain-goroutine activity pool.

![Worker-internal SDK mechanics sequence diagram](diagrams/05-worker-internal-sequence.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
sequenceDiagram
    autonumber
    participant Matching as Matching Service
    participant WFPoller as Workflow Task Poller
    participant Engine as Deterministic Engine + Sticky Cache
    participant UserWF as GreetingWorkflow (user code)
    participant ActPoller as Activity Task Poller
    participant Pool as Activity Goroutine Pool
    participant UserAct as ComposeGreeting (user code)
    participant FE as Frontend Service

    Note over WFPoller,UserWF: Workflow Task #1 — no cache entry yet for this RunId
    Matching-->>WFPoller: task A, history[1..3]
    WFPoller->>Engine: hand off task A
    Engine->>Engine: cache miss for RunId -> create new coroutine dispatcher
    Engine->>UserWF: invoke GreetingWorkflow(ctx, "Temporal")
    UserWF->>UserWF: workflow.GetLogger(ctx).Info("GreetingWorkflow started")
    UserWF->>UserWF: workflow.WithActivityOptions(StartToCloseTimeout=10s)
    UserWF->>Engine: workflow.ExecuteActivity(ctx, ComposeGreeting, "Temporal")
    Engine-->>UserWF: Future (unresolved)
    UserWF->>Engine: future.Get(ctx, &result)
    Engine->>Engine: coroutine yields (blocked on Future) -> decision loop ends
    Engine->>Engine: store coroutine state in sticky cache, keyed by RunId
    Engine-->>WFPoller: commands = [ScheduleActivityTask]
    WFPoller->>FE: RespondWorkflowTaskCompleted(TaskToken A, commands)

    Note over ActPoller,UserAct: Activity Task — plain goroutine, no determinism constraints
    Matching-->>ActPoller: task B, ComposeGreeting("Temporal")
    ActPoller->>Pool: dispatch to a pool goroutine
    Pool->>UserAct: ComposeGreeting(ctx, "Temporal")
    UserAct->>UserAct: activity.GetLogger(ctx).Info("Composing greeting")
    UserAct-->>Pool: return "Hello, Temporal!", nil
    Pool-->>ActPoller: result ready
    ActPoller->>FE: RespondActivityTaskCompleted(TaskToken B, "Hello, Temporal!")

    Note over WFPoller,UserWF: Workflow Task #2 — sticky cache HIT, no replay needed
    Matching-->>WFPoller: task C, incremental history[7..9]
    WFPoller->>Engine: hand off task C
    Engine->>Engine: cache hit for RunId -> reuse existing coroutine state
    Engine->>Engine: feed ActivityTaskCompleted result into the pending Future
    Engine->>UserWF: unblock future.Get() -> result = "Hello, Temporal!"
    UserWF->>UserWF: workflow.GetLogger(ctx).Info("GreetingWorkflow completed")
    UserWF-->>Engine: return "Hello, Temporal!", nil
    Engine->>Engine: coroutine finished -> decision loop ends
    Engine-->>WFPoller: commands = [CompleteWorkflowExecution]
    WFPoller->>FE: RespondWorkflowTaskCompleted(TaskToken C, commands)
```

</details>

Key SDK rules this diagram illustrates:

- **Workflow code runs in a deterministic coroutine dispatcher.** It must produce identical commands, in identical order, every time it is (re-)executed against the same history — this is what makes replay safe.
- **Activity code runs in an ordinary goroutine.** `ComposeGreeting` is free to do real I/O, call `time.Now()`, generate random numbers, etc. None of that is allowed inside `GreetingWorkflow` itself.
- **The sticky cache is a performance optimization, not a correctness requirement.** A cache hit skips replay entirely; a cache miss (shown in the [Appendix](#9-appendix-paths-not-exercised-by-this-run)) falls back to full replay from event 1.

---

## 7. Resulting Event History

Everything above ultimately produces one linear, append-only **Event History**
for `WorkflowId=greeting-workflow`. This is exactly what you'd see running
`temporal workflow show --workflow-id greeting-workflow` or opening the run in
the Web UI at `http://localhost:8233`.

| # | Event Type | Key attributes | Appended when |
|---|---|---|---|
| 1 | `WorkflowExecutionStarted` | `WorkflowType=GreetingWorkflow`, `Input=["Temporal"]`, `TaskQueue=greeting-task-queue` | Starter calls `ExecuteWorkflow` |
| 2 | `WorkflowTaskScheduled` | `TaskQueue=greeting-task-queue` (normal) | Immediately after event 1 |
| 3 | `WorkflowTaskStarted` | `Identity=<worker host:pid>` | Matching dispatches task A to the Worker |
| 4 | `WorkflowTaskCompleted` | `ScheduledEventId=2`, `StartedEventId=3` | Worker calls `RespondWorkflowTaskCompleted` |
| 5 | `ActivityTaskScheduled` | `ActivityType=ComposeGreeting`, `Input=["Temporal"]`, `StartToCloseTimeout=10s` | Result of processing the `ScheduleActivityTask` command |
| 6 | `ActivityTaskStarted` | `Identity=<worker host:pid>`, `Attempt=1` | Matching dispatches task B to the Worker |
| 7 | `ActivityTaskCompleted` | `Result=["Hello, Temporal!"]` | Worker calls `RespondActivityTaskCompleted` |
| 8 | `WorkflowTaskScheduled` | `TaskQueue=<sticky queue>` | Immediately after event 7 (a Future can now resolve) |
| 9 | `WorkflowTaskStarted` | `Identity=<same worker>` | Matching dispatches task C via the sticky partition |
| 10 | `WorkflowTaskCompleted` | `ScheduledEventId=8`, `StartedEventId=9` | Worker calls `RespondWorkflowTaskCompleted` |
| 11 | `WorkflowExecutionCompleted` | `Result=["Hello, Temporal!"]` | Result of processing the `CompleteWorkflowExecution` command |

All inputs/outputs (`"Temporal"`, `"Hello, Temporal!"`) are serialized to
Payload messages by the default `DataConverter` (JSON encoding) before crossing
the gRPC boundary and before being written to `temporal.db`.

---

## 8. Workflow execution state diagram

![Workflow execution state diagram](diagrams/06-workflow-state-diagram.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
stateDiagram-v2
    [*] --> Running: StartWorkflowExecution (event 1)
    Running --> Running: Workflow Task / Activity Task cycle (events 2-10)
    Running --> Completed: CompleteWorkflowExecution command (event 11)
    Running --> Failed: workflow function returns an error
    Running --> TimedOut: WorkflowExecutionTimeout exceeded
    Running --> Terminated: TerminateWorkflowExecution called externally
    Running --> ContinuedAsNew: workflow.NewContinueAsNewError
    Completed --> [*]
    Failed --> [*]
    TimedOut --> [*]
    Terminated --> [*]
    ContinuedAsNew --> [*]

    note right of Completed
        Path taken by this example:
        result = "Hello, Temporal!"
    end note
```

</details>

---

## 9. Appendix: paths not exercised by this run

These are shown for completeness ("as applicable") because they involve the
same components and are one config change or one bad day away from happening —
but none of them occur in the actual `GreetingWorkflow("Temporal")` run traced
above.

### 9.1 Activity retry (if `ComposeGreeting` returned an error)

`greeting/workflows.go` sets only `StartToCloseTimeout`; no explicit
`RetryPolicy` is configured, so Temporal applies its **default** retry policy:
1s initial interval, 2.0 backoff coefficient, 100s maximum interval, unlimited
attempts (bounded only by the timeout).

![Activity retry appendix diagram](diagrams/07-appendix-activity-retry.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
sequenceDiagram
    autonumber
    participant Worker
    participant FE as Frontend Service
    participant History as History Service
    participant Matching as Matching Service

    Note over Worker,Matching: Hypothetical — ComposeGreeting never actually fails in this example
    Worker->>FE: RespondActivityTaskFailed(TaskToken, error)
    FE->>History: RespondActivityTaskFailed
    History->>History: append ActivityTaskFailed, consult RetryPolicy (default 1s, x2 backoff, 100s cap, unlimited attempts)
    History->>History: start a backoff timer instead of rescheduling immediately
    History->>Matching: AddActivityTask (after the timer fires, Attempt=2)
    Matching-->>Worker: PollActivityTaskQueue response (Attempt 2)
    Note over Worker,Matching: Repeats until success, StartToCloseTimeout, or a non-retryable error
    Note over Worker,History: If attempts/time are exhausted, ActivityTaskFailed becomes terminal
    Note over Worker,History: ExecuteActivity(...).Get(ctx, &result) returns that error to GreetingWorkflow
    Note over Worker,History: GreetingWorkflow propagates it, producing WorkflowExecutionFailed
```

</details>

### 9.2 Cold-cache replay (if the Worker restarted or the sticky cache was evicted)

![Cold-cache replay appendix diagram](diagrams/08-appendix-cold-cache-replay.svg)

<details>
<summary>Mermaid source</summary>

```mermaid
sequenceDiagram
    autonumber
    participant Matching as Matching Service
    participant WFPoller as Workflow Task Poller (same or a brand-new Worker process)
    participant Engine as Deterministic Engine
    participant UserWF as GreetingWorkflow (user code)
    participant FE as Frontend Service

    Note over Matching,UserWF: Hypothetical — only happens if the sticky cache entry is gone
    Matching-->>WFPoller: task, FULL history[1..9] (sticky match failed, fell back to the normal queue)
    WFPoller->>Engine: hand off task
    Engine->>Engine: cache miss for RunId -> create a new coroutine dispatcher
    Engine->>UserWF: invoke GreetingWorkflow(ctx, "Temporal") from the top, in REPLAY mode
    UserWF->>UserWF: workflow.GetLogger(ctx).Info(...) -> replayed, no duplicate log side effects emitted
    UserWF->>Engine: workflow.ExecuteActivity(ctx, ComposeGreeting, "Temporal")
    Engine->>Engine: find the matching ActivityTaskScheduled/Completed pair already in history
    Engine-->>UserWF: Future resolves immediately with the recorded result (activity NOT re-run)
    UserWF->>UserWF: log "GreetingWorkflow completed" -> return the result
    Engine->>Engine: replay complete, coroutine caught up to live history
    Engine-->>WFPoller: commands = [CompleteWorkflowExecution] (identical to the original decision)
    WFPoller->>FE: RespondWorkflowTaskCompleted(commands)
    Note over Engine: Determinism requirement - replay must reissue the exact same commands, in the exact same order, as the original run
```

</details>

---

## 10. How to observe this yourself

1. `docker compose up -d` — starts the Temporal Cluster (Frontend + History + Matching + Persistence, all in one container).
2. `go run ./worker` — starts the Worker; watch it log `Worker started running on "greeting-task-queue"...`.
3. `go run ./starter` — starts the workflow and blocks until it prints `Workflow result: Hello, Temporal!`.
4. Open `http://localhost:8233`, find `greeting-workflow`, and inspect its **Event History** tab — it will match the table in [section 7](#7-resulting-event-history) exactly (11 events).
5. Optional: with the `temporal` CLI installed, run `temporal workflow show --workflow-id greeting-workflow` for the same history as text.
