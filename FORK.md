# What this fork adds

This fork of OpenWorkflow adds a few features to the **SQLite backend**. Every
addition is additive: upstream behavior and upstream tests are unchanged, and
signals work exactly as upstream documents them. On **Postgres** each new
option or method is rejected with a "does not support" error rather than
silently ignored.

| Feature                                                  | Section |
| -------------------------------------------------------- | ------- |
| `concurrencyKey`: one run at a time per key              | 1       |
| `id`: caller-chosen run IDs                              | 2       |
| `BackendSqlite.fromDatabase` and `createWorkflowRunSync` | 3       |
| Messages: buffered, run-addressed, read once in order    | 4       |
| Keys and IDs on child runs                               | 5       |
| `step.startWorkflow`: one-way starts                     | 6       |
| Keys and an overlap rule on schedules                    | 7       |
| List and cancel runs by key                              | 8       |
| `forkMigrations()`: the fork's schema, versioned apart   | 9       |

The examples assume:

```ts
import { OpenWorkflow } from "openworkflow";
import { BackendSqlite } from "openworkflow/sqlite";

const backend = BackendSqlite.connect("./app.db");
const ow = new OpenWorkflow({ backend });
```

## 1. Concurrency keys

```ts
await turn.run({ name: "a1" }, { concurrencyKey: "company:acme" });
await turn.run({ name: "a2" }, { concurrencyKey: "company:acme" }); // runs after a1
await turn.run({ name: "b1" }, { concurrencyKey: "company:other" }); // runs alongside a1
```

**Guarantees.** Runs that share a key never execute at the same time. A run
takes its key when it is first claimed (not when it is created) and holds it
until it finishes: completed, permanently failed, canceled, or past its
deadline. Waiting runs of a key are claimed pending first, then by
`availableAt`, creation time and insertion order, so a delayed run does not
block an earlier-due run of its key. The key is shared across all workflows in
the backend namespace. It holds across processes sharing one database file (a
partial unique index backs the claim query). A rerun keeps its source run's
key.

**Does not.** A started run keeps its key while it sleeps, waits for a signal,
message or child, retries after a failed step, or has a lease that has not yet
expired; other runs of the key wait all that time (a wait's default timeout is
one year). A deadline releases the key on the next worker poll after it
passes. The empty string is a real key. There is no deadlock detection: two
keyed runs that wait on each other wait forever.

**Postgres.** `createWorkflowRun` rejects a key.

## 2. Run IDs

```ts
const handle = await turn.run({ name: "x" }, { id: "event:123" });
const again = await turn.run({ name: "x" }, { id: "event:123" }); // same run
```

**Guarantees.** Creating a run with an ID that exists creates nothing and
returns the existing run, whatever its state and whatever the other options
say. IDs are unique per namespace across all workflows, and stay reserved for
as long as the run is kept (this backend never deletes runs).

**Does not.** A rerun gets a new generated ID.

**Postgres.** `createWorkflowRun` rejects an ID.

## 3. The application's own connection and transactions

```ts
import { DatabaseSync } from "node:sqlite";

// or Database from "bun:sqlite"

const db = new DatabaseSync("./app.db");
db.exec("PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;");
const backend = BackendSqlite.fromDatabase(db); // migrates; stop() leaves db open

db.exec("BEGIN IMMEDIATE");
insertEvent.run("event-1"); // the application's own write
const run = backend.createWorkflowRunSync({
  workflowName: "turn",
  version: null,
  idempotencyKey: null,
  config: {},
  context: null,
  input: null,
  parentStepAttemptNamespaceId: null,
  parentStepAttemptId: null,
  availableAt: null,
  deadlineAt: null,
  id: "event:1",
  concurrencyKey: "company:acme",
});
backend.sendMessageSync({
  workflowRunId: run.id,
  name: "event",
  data: { id: "event-1" },
  idempotencyKey: null,
});
db.exec("COMMIT"); // all three commit or roll back together
```

**Guarantees.** `fromDatabase` builds the backend on an open connection and
runs migrations unless `runMigrations: false`. `createWorkflowRunSync` and
`sendMessageSync` (section 4) write without opening a transaction, so they
commit or roll back with the caller's. Creating a run by ID and sending it its
first message in one transaction gives the effect of Temporal's
signal-with-start. A worker running several runs at once on the same
connection stays correct alongside the application's transactions.

**Does not.** `fromDatabase` sets no PRAGMAs. **The application's
transactions on this connection must be synchronous** (no `await` between
`BEGIN` and `COMMIT`). The backend runs its own transactions on the same
connection whenever the application yields. If the application yields inside
its transaction, the worker's claims fail with "cannot start a transaction
within a transaction", and writes from a run that is already executing join
the application's transaction and are lost if it rolls back. Inside the
application's transaction, use only the synchronous methods: async `ow.*` and
backend calls that open their own transaction (such as `ow.sendMessage`)
fail with the same error, and the rest join the application's transaction.
`createWorkflowRunSync` does not accept `idempotencyKey` (pass `id`), and
skips input schema validation and tracing.

**Postgres.** These methods exist only on `BackendSqlite`.

## 4. Messages

This implements upstream proposal
[#894](https://github.com/openworkflowdev/openworkflow/issues/894).

```ts
// in a workflow: wait for a reply, with a time limit
const decision = await step.waitForMessage<{ approved: boolean }>({
  name: "approval",
  timeout: "7d",
});
if (decision === null) return { status: "expired" };

// in application code: safe before the run reaches the wait, while no worker
// is running, or twice
await ow.sendMessage({
  workflowRunId: run.workflowRun.id,
  name: "approval",
  data: { approved: true },
  idempotencyKey: `approval:${purchaseId}`,
});

// from another workflow, recorded as a step so a replay does not resend it
await step.sendMessage({ workflowRunId: childId, name: "go", data: {} });

// pending and consumed messages, with the step that read each
const page = await ow.listMessages({ workflowRunId: run.workflowRun.id });
```

| API                                                                      | Returns                                         |
| ------------------------------------------------------------------------ | ----------------------------------------------- |
| `ow.sendMessage({ workflowRunId, name, data?, idempotencyKey? })`        | `{ messageId }`                                 |
| `step.sendMessage({ workflowRunId, name, data?, stepName? })`            | `{ messageId }`                                 |
| `step.waitForMessage({ name, stepName?, timeout?, schema? })`            | `{ data } \| null` (null on timeout)            |
| `ow.listMessages({ workflowRunId, limit?, after?, before? })`            | a page of messages                              |
| `backend.sendMessageSync({ workflowRunId, name, data, idempotencyKey })` | `{ messageId }`; joins the caller's transaction |

**Guarantees.** A message is addressed to one run and stored until a wait in
that run reads it. Each wait takes the oldest unread message with its name, in
the order they were sent. The claim is tied to the wait's step attempt, so a
replay after a crash receives the same message, and no message is read twice.
Idempotency keys are unique per run with no expiry: a repeated send stores
nothing and returns the original message's ID. A message to a run that exists
but has not started (pending, delayed, or queued behind its key) is stored. A
send wakes the run only if it is parked waiting for that name; the race
between "the wait found nothing" and "the run parks" is closed where the run
parks. Reruns start with an empty inbox; completed waits are copied with their
outputs like other steps. `step.sendMessage` derives its idempotency key from
the run and step, so a replay sends nothing new. The step name defaults to the
message name; two waits on one name in a run are fine (each takes the next
message).

**Does not.** A send to an unknown run throws, and a send to a finished run
throws and stores nothing (a retry of an earlier send with its idempotency key
still returns the original). Unread messages stay on a finished run. A message
that arrives after the timeout but before the wait is replayed is still taken
by that wait, as with signals. `listMessages` orders by creation time; within
one millisecond its order may differ from the order of reading.

**Postgres.** `sendMessage`, `consumeMessage` and `listMessages` are rejected.

## 5. Keys and IDs on child runs

```ts
// a run with no key changes company data only through short keyed children
const draft = await step.runWorkflow(
  writeDraft.spec,
  { company: "acme" },
  { concurrencyKey: "company:acme", id: `draft:${applicationId}` },
);
```

**Guarantees.** The child is created with the key and queues behind it like any
other run; the parent waits for its result as usual. A parent with no key holds
no key while it waits, so the company is not blocked while a long process
waits. If `id` names a run that already exists, the parent waits on that run
and nothing is created; any parent waiting on a run is woken when it finishes.
A parent replayed after a crash links the same child.

**Does not.** A child with its parent's key is refused with an error that is
not retried (the parent holds its key while it waits, so the child could never
run). A parent with its own key that starts a child with a different key keeps
its own key while it waits. Two keyed runs that wait on each other through
children deadlock; nothing detects it. Canceling a parent does not cancel the
child it waits on.

**Postgres.** The child is rejected when created.

## 6. One-way starts

```ts
const followUpId = await step.startWorkflow(
  followUp.spec,
  { company: "acme" },
  { concurrencyKey: "company:acme", availableAt: "3d" },
);
```

Options: `name` (step name, default the workflow name), `id`,
`concurrencyKey`, `availableAt` (a `Date` or duration, for a delayed start)
and `deadlineAt`. Returns the started run's ID.

**Guarantees.** Recorded as a step: a replay returns the same ID and creates
nothing. It can never create two runs: without an `id`, the ID is derived from
the starting run's ID and the step name, so a crash at any point leads back to
the same run. The started run is independent: its completion, failure or
cancellation never affects the starter. Its key may be the starter's own
(nothing waits, so it runs after the starter finishes).

**Does not.** The starter is not told when the started run finishes; send it a
message if it needs to know. A rerun of the starter starts new runs.

**Postgres.** Rejected, because the started run always has an ID.

## 7. Schedules

```ts
await report.schedule(input, {
  cron: "0 9 * * *",
  concurrencyKey: "company:acme",
  overlap: "skip",
});
// or in the workflow's definition:
// { name: "report", schedules: [{ cron: "0 9 * * *", overlap: "skip" }] }
```

**Guarantees.** Runs created by the schedule carry its key and queue behind it.
`overlap` is `"allow"` (the default, upstream behavior) or `"skip"`: with
`"skip"`, a tick that comes due while an earlier run of the same schedule is
unfinished (pending, running or parked) creates nothing, and the schedule moves
on to its next time. Each scheduled run's ID is derived from the schedule ID and
the tick's time, so a tick creates at most one run however often it is
processed. The options are kept through pause and resume, and are returned on
`WorkflowSchedule`.

**Does not.** Re-declaring a schedule replaces all its options (last write
wins), so leaving them out clears them. `"skip"` does not queue the skipped
tick for later.

**Postgres.** A key or `"skip"` is rejected; Postgres scheduled runs keep
random IDs.

## 8. List and cancel by key

```ts
const page = await backend.listWorkflowRuns({ concurrencyKey: "company:acme" });

// from inside a keyed run, drop the rest of its key's pending work
const { workflowRunIds } = await ow.cancelWorkflowRunsByConcurrencyKey(
  "company:acme",
  { exceptWorkflowRunId: run.id },
);
```

**Guarantees.** `WorkflowRun.concurrencyKey` holds the run's key.
`listWorkflowRuns` filters by key. `cancelWorkflowRunsByConcurrencyKey`
cancels every unfinished run with the key (pending, delayed, running or
parked) in one transaction, returns their IDs, and leaves the excepted run
alone. Canceled runs free the key, and wake any parent waiting on them.
Repeating the call changes nothing.

**Does not.** `concurrencyKey` is absent (not `null`) on a run with no key. A
run that is executing when canceled may finish its current step, as with a
single cancel.

**Postgres.** The filter and the cancel are rejected.

## 9. Fork migrations

The fork's schema changes live in `forkMigrations()` in
`packages/openworkflow/sqlite/sqlite.ts`, recorded in their own table,
`openworkflow_fork_migrations`, so upstream's migration list and numbering are
untouched. `migrate()` applies pending upstream migrations, then pending fork
migrations, in one transaction. An existing database upgrades in place.

| Version | Adds                                                                         |
| ------- | ---------------------------------------------------------------------------- |
| 0       | `workflow_runs.concurrency_key` and the one-started-run-per-key unique index |
| 1       | the `workflow_messages` table and its indexes                                |
| 2       | `workflow_schedules.concurrency_key` and `workflow_schedules.overlap`        |

Fork migration 2 alters the table upstream migration 7 creates, which
`migrate()` always applies first. Postgres has no fork migrations.
