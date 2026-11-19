import { OpenWorkflow } from "../client/client.js";
import type { Logger } from "../core/logger.js";
import type { WorkflowRun } from "../core/workflow-run.js";
import type { Worker } from "../worker/worker.js";
import { BackendSqlite } from "./backend.js";
import { newDatabase } from "./sqlite.js";
import { randomUUID } from "node:crypto";
import { existsSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout } from "node:timers/promises";
import { describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

function tempDatabasePath(): string {
  return path.join(tmpdir(), `openworkflow-fork-${randomUUID()}.db`);
}

function removeDatabaseFiles(dbPath: string): void {
  for (const file of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) {
    if (existsSync(file)) unlinkSync(file);
  }
}

async function tickUntil(
  worker: Worker,
  backend: BackendSqlite,
  workflowRunId: string,
  expected: Partial<WorkflowRun>,
): Promise<void> {
  await vi.waitFor(
    async () => {
      await worker.tick();
      expect(await backend.getWorkflowRun({ workflowRunId })).toMatchObject(
        expected,
      );
    },
    { timeout: 5000, interval: 5 },
  );
}

/**
 * The run's parked wait: the name of its running step once no worker holds it.
 * @param backend - Backend holding the run
 * @param workflowRunId - Run to inspect
 * @returns Name of the running step, or null when the run is not parked
 */
async function parkedOn(
  backend: BackendSqlite,
  workflowRunId: string,
): Promise<string | null> {
  const run = await backend.getWorkflowRun({ workflowRunId });
  if (run?.status !== "running" || run.workerId !== null) return null;
  const steps = await backend.listStepAttempts({ workflowRunId });
  return steps.data.find((step) => step.status === "running")?.stepName ?? null;
}

async function tickUntilParkedOn(
  worker: Worker,
  backend: BackendSqlite,
  workflowRunId: string,
  stepName: string,
): Promise<void> {
  await vi.waitFor(
    async () => {
      await worker.tick();
      expect(await parkedOn(backend, workflowRunId)).toBe(stepName);
    },
    { timeout: 5000, interval: 5 },
  );
}

describe("fork acceptance (SQLite)", () => {
  test.each(["after", "before"] as const)(
    "a keyless run acts for a company through keyed children, with the approval sent %s it waits",
    async (approvalTiming) => {
      const dbPath = tempDatabasePath();
      const executions = { draft: 0, act: 0, other: 0 };

      // Each "process" is a fresh connection, client and worker on one file.
      function open() {
        const backend = BackendSqlite.connect(dbPath);
        const ow = new OpenWorkflow({ backend, logger: quietLogger });
        const draft = ow.defineWorkflow<{ company: string }, string>(
          { name: "draft" },
          async ({ input, step }) =>
            await step.run({ name: "write" }, () => {
              executions.draft++;
              return `draft for ${input.company}`;
            }),
        );
        const act = ow.defineWorkflow<{ draft: string }, string>(
          { name: "act" },
          async ({ input, step }) =>
            await step.run({ name: "send" }, () => {
              executions.act++;
              return `sent ${input.draft}`;
            }),
        );
        const other = ow.defineWorkflow({ name: "other" }, () => {
          executions.other++;
        });
        const application = ow.defineWorkflow(
          { name: "application" },
          async ({ step }) => {
            const written = await step.runWorkflow(
              draft.workflow.spec,
              { company: "acme" },
              { concurrencyKey: "company:acme" },
            );
            const approval = await step.waitForMessage<{ approved: boolean }>({
              name: "approval",
              timeout: "7d",
            });
            if (!approval?.data.approved) return { outcome: "not approved" };
            const sent = await step.runWorkflow(
              act.workflow.spec,
              { draft: written },
              { concurrencyKey: "company:acme" },
            );
            return { outcome: sent };
          },
        );
        return { backend, ow, application, other, worker: ow.newWorker() };
      }
      async function restart(app: ReturnType<typeof open>) {
        await app.worker.stop();
        await app.backend.stop();
        return open();
      }
      const approve = (app: ReturnType<typeof open>, workflowRunId: string) =>
        app.ow.sendMessage({
          workflowRunId,
          name: "approval",
          data: { approved: true },
          idempotencyKey: "approval:acme",
        });

      let app = open();
      try {
        // 1. a run with no key starts a keyed child to write the draft
        const { workflowRun } = await app.application.run();
        const id = workflowRun.id;
        expect(workflowRun.concurrencyKey).toBeUndefined();
        if (approvalTiming === "before") await approve(app, id);
        await tickUntilParkedOn(app.worker, app.backend, id, "draft");
        app = await restart(app);

        if (approvalTiming === "after") {
          // 2. while it waits for approval, the company is not blocked
          await tickUntilParkedOn(app.worker, app.backend, id, "approval");
          app = await restart(app);
          const otherRun = await app.other.run(undefined, {
            concurrencyKey: "company:acme",
          });
          await tickUntil(app.worker, app.backend, otherRun.workflowRun.id, {
            status: "completed",
          });
          expect(await parkedOn(app.backend, id)).toBe("approval");

          // 3. the approval arrives while it waits
          await approve(app, id);
          app = await restart(app);
        }

        // 4. it acts on the approval through another keyed child
        await tickUntilParkedOn(app.worker, app.backend, id, "act");
        app = await restart(app);
        await tickUntil(app.worker, app.backend, id, {
          status: "completed",
          output: { outcome: "sent draft for acme" },
        });

        // 5. no child ran twice across the restarts
        expect(executions).toEqual({
          draft: 1,
          act: 1,
          other: approvalTiming === "after" ? 1 : 0,
        });
        for (const workflowName of ["draft", "act"]) {
          const runs = await app.backend.listWorkflowRuns({ workflowName });
          expect(runs.data).toHaveLength(1);
        }
        const inbox = await app.ow.listMessages({ workflowRunId: id });
        expect(inbox.data).toHaveLength(1);
      } finally {
        await app.worker.stop();
        await app.backend.stop();
        removeDatabaseFiles(dbPath);
      }
    },
  );

  test("a shared connection stays correct with a concurrent worker and the application's own transactions", async () => {
    const dbPath = tempDatabasePath();
    // node:sqlite DatabaseSync under Node, bun:sqlite Database under Bun
    const db = newDatabase(dbPath);
    const backend = BackendSqlite.fromDatabase(db);
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    db.exec(`CREATE TABLE "events" ("id" TEXT NOT NULL PRIMARY KEY)`);
    const insertEvent = db.prepare(`INSERT INTO "events" ("id") VALUES (?)`);
    const active = new Map<string, number>();
    let maxActivePerKey = 0;
    ow.defineWorkflow<{ key: string }, unknown>(
      { name: "event" },
      async ({ input, step }) => {
        await step.run({ name: "handle" }, async () => {
          const now = (active.get(input.key) ?? 0) + 1;
          active.set(input.key, now);
          maxActivePerKey = Math.max(maxActivePerKey, now);
          await setTimeout(1);
          active.set(input.key, now - 1);
        });
        return await step.waitForMessage({ name: "event", timeout: "1h" });
      },
    );
    const worker = ow.newWorker({ concurrency: 4 });
    const committed: string[] = [];

    try {
      await worker.start();
      for (let n = 0; n < 40; n++) {
        const id = `event:${String(n)}`;
        // the application's own synchronous transaction
        db.exec("BEGIN IMMEDIATE");
        insertEvent.run(id);
        backend.createWorkflowRunSync({
          workflowName: "event",
          version: null,
          idempotencyKey: null,
          config: {},
          context: null,
          input: { key: `company:${String(n % 3)}` },
          parentStepAttemptNamespaceId: null,
          parentStepAttemptId: null,
          availableAt: null,
          deadlineAt: null,
          id,
          concurrencyKey: `company:${String(n % 3)}`,
        });
        backend.sendMessageSync({
          workflowRunId: id,
          name: "event",
          data: { n },
          idempotencyKey: null,
        });
        if (n % 5 === 4) {
          db.exec("ROLLBACK");
        } else {
          db.exec("COMMIT");
          committed.push(id);
        }
        // yield, so the worker interleaves its own transactions
        await setTimeout(n % 2);
      }

      await vi.waitFor(
        async () => {
          for (const id of committed) {
            const run = await backend.getWorkflowRun({ workflowRunId: id });
            expect(run?.status).toBe("completed");
          }
        },
        { timeout: 10_000, interval: 20 },
      );
      for (const id of committed) {
        const run = await backend.getWorkflowRun({ workflowRunId: id });
        expect(run?.output).toEqual({
          data: { n: Number(id.split(":")[1]) },
        });
      }
      // safety: COUNT(*) returns a numeric count column for this query.
      const events = db
        .prepare(`SELECT COUNT(*) AS "count" FROM "events"`)
        .get() as { count: number } | undefined;
      expect(events?.count).toBe(committed.length);
      const runs = await backend.listWorkflowRuns({ limit: 100 });
      expect(runs.data).toHaveLength(committed.length);
      expect(maxActivePerKey).toBe(1);
    } finally {
      await worker.stop();
      await backend.stop();
      db.close();
      removeDatabaseFiles(dbPath);
    }
  });

  test("canceling a parent that waits on a child does not cancel the child", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const childGate = Promise.withResolvers<void>();
    const child = ow.defineWorkflow({ name: "child" }, async ({ step }) => {
      await step.run({ name: "work" }, async () => {
        await childGate.promise;
      });
      return "child done";
    });
    const parent = ow.defineWorkflow(
      { name: "parent" },
      async ({ step }) => await step.runWorkflow(child.workflow.spec),
    );
    const worker = ow.newWorker({ concurrency: 2 });
    try {
      const { workflowRun } = await parent.run();
      await tickUntilParkedOn(worker, backend, workflowRun.id, "child");
      const children = await backend.listWorkflowRuns({
        workflowName: "child",
      });
      const childId = children.data[0]?.id ?? "";

      await ow.cancelWorkflowRun(workflowRun.id);
      await tickUntil(worker, backend, childId, { status: "running" });

      childGate.resolve();
      await tickUntil(worker, backend, childId, {
        status: "completed",
        output: "child done",
      });
      expect(
        await backend.getWorkflowRun({ workflowRunId: workflowRun.id }),
      ).toMatchObject({ status: "canceled" });
    } finally {
      childGate.resolve();
      await worker.stop();
      await backend.stop();
    }
  });
});
