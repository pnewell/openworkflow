import { OpenWorkflow } from "../client/client.js";
import type { CreateWorkflowRunParams } from "../core/backend.js";
import type { Logger } from "../core/logger.js";
import { createMessageWaitContext } from "../core/step-attempt.js";
import type { WorkflowRun } from "../core/workflow-run.js";
import type { Worker } from "../worker/worker.js";
import { BackendSqlite } from "./backend.js";
import {
  Database,
  forkMigrations,
  migrate,
  migrations,
  newDatabase,
} from "./sqlite.js";
import { afterEach, describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

const T0 = new Date("2026-01-02T12:00:00.000Z");
const HOUR = 60 * 60 * 1000;

function runParams(
  overrides: Partial<CreateWorkflowRunParams> = {},
): CreateWorkflowRunParams {
  return {
    workflowName: "approval",
    version: null,
    idempotencyKey: null,
    config: {},
    context: null,
    input: null,
    parentStepAttemptNamespaceId: null,
    parentStepAttemptId: null,
    availableAt: null,
    deadlineAt: null,
    ...overrides,
  };
}

function count(db: Database, table: string): number {
  // safety: COUNT(*) returns a numeric count column for this query.
  const row = db.prepare(`SELECT COUNT(*) AS "count" FROM "${table}"`).get() as
    { count: number } | undefined;
  return row?.count ?? 0;
}

/**
 * A client whose "approval" workflow waits for one "approval" message.
 * @param backend - Backend to use
 * @returns Client, workflow and worker
 */
function approvalClient(backend: BackendSqlite) {
  const ow = new OpenWorkflow({ backend, logger: quietLogger });
  const approval = ow.defineWorkflow<void, unknown>(
    { name: "approval" },
    async ({ step }) =>
      await step.waitForMessage({ name: "approval", timeout: "7d" }),
  );
  return { ow, approval, worker: ow.newWorker() };
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

const parked: Partial<WorkflowRun> = { status: "running", workerId: null };

async function output(backend: BackendSqlite, workflowRunId: string) {
  const run = await backend.getWorkflowRun({ workflowRunId });
  return run?.output;
}

async function messages(backend: BackendSqlite, workflowRunId: string) {
  const page = await backend.listMessages({ workflowRunId });
  return page.data;
}

describe("messages (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  describe("a message is received whenever it is sent", () => {
    test("to a pending run that has not started", async () => {
      const backend = BackendSqlite.connect(":memory:");
      const { ow, approval, worker } = approvalClient(backend);
      try {
        const handle = await approval.run();
        await ow.sendMessage({
          workflowRunId: handle.workflowRun.id,
          name: "approval",
          data: { approved: true },
        });

        await tickUntil(worker, backend, handle.workflowRun.id, {
          status: "completed",
        });
        expect(await output(backend, handle.workflowRun.id)).toEqual({
          data: { approved: true },
        });
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("to a delayed run, which receives it once due", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(T0);
      const backend = BackendSqlite.connect(":memory:");
      const { ow, approval, worker } = approvalClient(backend);
      try {
        const handle = await approval.run(undefined, { availableAt: "1h" });
        await ow.sendMessage({
          workflowRunId: handle.workflowRun.id,
          name: "approval",
          data: "early",
        });
        expect(await worker.tick()).toBe(0);

        vi.setSystemTime(new Date(T0.getTime() + HOUR + 1));
        await tickUntil(worker, backend, handle.workflowRun.id, {
          status: "completed",
          output: { data: "early" },
        });
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("while the run is parked before the wait, without waking it", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(T0);
      const backend = BackendSqlite.connect(":memory:");
      const ow = new OpenWorkflow({ backend, logger: quietLogger });
      const workflow = ow.defineWorkflow(
        { name: "approval" },
        async ({ step }) => {
          await step.sleep("pause", "1h");
          return await step.waitForMessage({ name: "approval" });
        },
      );
      const worker = ow.newWorker();
      try {
        const { workflowRun } = await workflow.run();
        await tickUntil(worker, backend, workflowRun.id, parked);
        const before = await backend.getWorkflowRun({
          workflowRunId: workflowRun.id,
        });

        await ow.sendMessage({
          workflowRunId: workflowRun.id,
          name: "approval",
          data: 1,
        });
        expect(
          await backend.getWorkflowRun({ workflowRunId: workflowRun.id }),
        ).toMatchObject({ availableAt: before?.availableAt });
        expect(await worker.tick()).toBe(0);

        vi.setSystemTime(new Date(T0.getTime() + HOUR + 1));
        await tickUntil(worker, backend, workflowRun.id, {
          status: "completed",
          output: { data: 1 },
        });
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("while the run is on an earlier step", async () => {
      const backend = BackendSqlite.connect(":memory:");
      const ow = new OpenWorkflow({ backend, logger: quietLogger });
      const workflow = ow.defineWorkflow(
        { name: "approval" },
        async ({ step, run }) => {
          await step.run({ name: "ask" }, async () => {
            await ow.sendMessage({
              workflowRunId: run.id,
              name: "approval",
              data: "mid-step",
            });
          });
          return await step.waitForMessage({ name: "approval" });
        },
      );
      const worker = ow.newWorker();
      try {
        const { workflowRun } = await workflow.run();
        await tickUntil(worker, backend, workflowRun.id, {
          status: "completed",
        });
        expect(await output(backend, workflowRun.id)).toEqual({
          data: "mid-step",
        });
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("between the wait step being created and the run parking", async () => {
      const backend = BackendSqlite.connect(":memory:");
      const { ow, approval, worker } = approvalClient(backend);
      const sleep = backend.sleepWorkflowRun.bind(backend);
      const sleepSpy = vi
        .spyOn(backend, "sleepWorkflowRun")
        .mockImplementationOnce(async (params) => {
          // the wait found no message; one arrives before the park commits
          await ow.sendMessage({
            workflowRunId: params.workflowRunId,
            name: "approval",
            data: "raced",
          });
          return await sleep(params);
        });
      try {
        const { workflowRun } = await approval.run();
        await tickUntil(worker, backend, workflowRun.id, {
          status: "completed",
          output: { data: "raced" },
        });
        // parked once, woken by the park itself rather than after 7 days
        expect(sleepSpy).toHaveBeenCalledTimes(1);
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("while no worker is running, waking the parked run", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(T0);
      const backend = BackendSqlite.connect(":memory:");
      const first = approvalClient(backend);
      const { workflowRun } = await first.approval.run();
      try {
        await tickUntil(first.worker, backend, workflowRun.id, parked);
      } finally {
        await first.worker.stop();
      }

      try {
        vi.setSystemTime(new Date(T0.getTime() + HOUR));
        await first.ow.sendMessage({
          workflowRunId: workflowRun.id,
          name: "approval",
          data: "offline",
        });
        // woken: due now rather than at the wait's 7-day timeout
        const woken = await backend.getWorkflowRun({
          workflowRunId: workflowRun.id,
        });
        expect(woken?.availableAt?.getTime()).toBeLessThanOrEqual(Date.now());

        const second = approvalClient(backend);
        await tickUntil(second.worker, backend, workflowRun.id, {
          status: "completed",
          output: { data: "offline" },
        });
        await second.worker.stop();
      } finally {
        await backend.stop();
      }
    });
  });

  test("two messages with one name are read in order by two waits", async () => {
    // both messages are created in the same millisecond
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const workflow = ow.defineWorkflow({ name: "items" }, async ({ step }) => {
      const a = await step.waitForMessage<string>({ name: "item" });
      const b = await step.waitForMessage<string>({ name: "item" });
      return [a?.data, b?.data];
    });
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await workflow.run();
      for (const data of ["first", "second", "third"]) {
        await ow.sendMessage({
          workflowRunId: workflowRun.id,
          name: "item",
          data,
        });
      }

      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        output: ["first", "second"],
      });

      const steps = await backend.listStepAttempts({
        workflowRunId: workflowRun.id,
      });
      const stepIds = new Map(
        steps.data.map((attempt) => [attempt.stepName, attempt.id]),
      );
      const listed = await ow.listMessages({ workflowRunId: workflowRun.id });
      expect(
        listed.data
          .map((message) => ({
            data: message.data,
            consumedBy: message.consumedByStepAttemptId,
            read: message.consumedAt !== null,
          }))
          .toSorted((x, y) =>
            JSON.stringify(x.data).localeCompare(JSON.stringify(y.data)),
          ),
      ).toEqual([
        { data: "first", consumedBy: stepIds.get("item"), read: true },
        { data: "second", consumedBy: stepIds.get("item:1"), read: true },
        { data: "third", consumedBy: null, read: false },
      ]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a repeated idempotency key stores one message per run", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const { ow, approval } = approvalClient(backend);
    try {
      const a = await approval.run();
      const b = await approval.run();
      const send = (workflowRunId: string, data: string) =>
        ow.sendMessage({
          workflowRunId,
          name: "approval",
          data,
          idempotencyKey: "approval:1",
        });

      const sent = await send(a.workflowRun.id, "once");
      expect(await send(a.workflowRun.id, "twice")).toEqual(sent);
      expect(await send(b.workflowRun.id, "other run")).not.toEqual(sent);

      const inbox = await messages(backend, a.workflowRun.id);
      expect(inbox.map((m) => m.data)).toEqual(["once"]);
      expect(await messages(backend, b.workflowRun.id)).toHaveLength(1);
    } finally {
      await backend.stop();
    }
  });

  test("a message to an unknown run throws", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const { ow } = approvalClient(backend);
    try {
      await expect(
        ow.sendMessage({ workflowRunId: "missing", name: "approval" }),
      ).rejects.toThrow("Workflow run missing does not exist");
    } finally {
      await backend.stop();
    }
  });

  test("a finished run rejects the message and stores nothing", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const { ow, approval, worker } = approvalClient(backend);
    try {
      const done = await approval.run();
      const sent = await ow.sendMessage({
        workflowRunId: done.workflowRun.id,
        name: "approval",
        idempotencyKey: "approval:1",
      });
      await tickUntil(worker, backend, done.workflowRun.id, {
        status: "completed",
      });
      const canceled = await approval.run();
      await canceled.cancel();

      await expect(
        ow.sendMessage({ workflowRunId: done.workflowRun.id, name: "late" }),
      ).rejects.toThrow(/with status completed/);
      await expect(
        ow.sendMessage({ workflowRunId: canceled.workflowRun.id, name: "x" }),
      ).rejects.toThrow(/with status canceled/);
      // a retry of the send that arrived in time still succeeds
      await expect(
        ow.sendMessage({
          workflowRunId: done.workflowRun.id,
          name: "approval",
          idempotencyKey: "approval:1",
        }),
      ).resolves.toEqual(sent);

      expect(await messages(backend, done.workflowRun.id)).toHaveLength(1);
      expect(await messages(backend, canceled.workflowRun.id)).toHaveLength(0);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a wait that times out returns null", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const workflow = ow.defineWorkflow(
      { name: "approval" },
      async ({ step }) =>
        await step.waitForMessage({ name: "approval", timeout: "1h" }),
    );
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await workflow.run();
      await tickUntil(worker, backend, workflowRun.id, parked);
      expect(await worker.tick()).toBe(0);

      vi.setSystemTime(new Date(T0.getTime() + HOUR));
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        output: null,
      });
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a crash after the claim gives the same message to the same step", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const { ow, approval, worker } = approvalClient(backend);
    try {
      const { workflowRun } = await approval.run();
      for (const n of [1, 2]) {
        await ow.sendMessage({
          workflowRunId: workflowRun.id,
          name: "approval",
          data: n,
        });
      }

      // a worker claims the run, creates the wait, claims a message, and dies
      await backend.claimWorkflowRun({
        workerId: "crashed",
        leaseDurationMs: 1000,
      });
      const wait = await backend.createStepAttempt({
        workflowRunId: workflowRun.id,
        workerId: "crashed",
        stepName: "approval",
        stepIndex: 0,
        kind: "message-wait",
        config: {},
        context: createMessageWaitContext(
          "approval",
          new Date(T0.getTime() + 7 * 24 * HOUR),
        ),
      });
      const claimed = await backend.consumeMessage({
        workflowRunId: workflowRun.id,
        stepAttemptId: wait.id,
        name: "approval",
      });
      expect(claimed).toMatchObject({
        data: 1,
        consumedByStepAttemptId: wait.id,
      });

      vi.setSystemTime(new Date(T0.getTime() + 2000));
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        attempts: 2,
        output: { data: 1 },
      });
      const inbox = await messages(backend, workflowRun.id);
      expect(
        inbox
          .map((m) => [m.data, m.consumedByStepAttemptId])
          .toSorted((x, y) => Number(x[0]) - Number(y[0])),
      ).toEqual([
        [1, wait.id],
        [2, null],
      ]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a rerun starts with an empty inbox and keeps completed waits", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    let fail = true;
    const workflow = ow.defineWorkflow(
      { name: "approval" },
      async ({ step }) => {
        const decision = await step.waitForMessage({ name: "approval" });
        await step.run(
          { name: "act", retryPolicy: { maximumAttempts: 1 } },
          () => {
            if (fail) throw new Error("not yet");
          },
        );
        return decision;
      },
    );
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await workflow.run();
      for (const data of ["used", "unread"]) {
        await ow.sendMessage({
          workflowRunId: workflowRun.id,
          name: "approval",
          data,
        });
      }
      await tickUntil(worker, backend, workflowRun.id, { status: "failed" });
      fail = false;

      const fromStart = await ow.rerunWorkflowRun(workflowRun.id);
      await tickUntil(worker, backend, fromStart.id, parked);
      expect(await messages(backend, fromStart.id)).toEqual([]);

      const fromAct = await ow.rerunWorkflowRun(workflowRun.id, {
        fromStep: "act",
      });
      await tickUntil(worker, backend, fromAct.id, {
        status: "completed",
        output: { data: "used" },
      });
      expect(await messages(backend, fromAct.id)).toEqual([]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("step.sendMessage is not resent on replay or after a crash", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const receiver = ow.defineWorkflow(
      { name: "receiver" },
      async ({ step }) => await step.waitForMessage({ name: "go" }),
    );
    const sender = ow.defineWorkflow<{ to: string }, unknown>(
      { name: "sender" },
      async ({ input, step }) => {
        const sent = await step.sendMessage({
          workflowRunId: input.to,
          name: "go",
          data: { from: "sender" },
        });
        await step.sleep("pause", "1h");
        return sent;
      },
    );
    const worker = ow.newWorker();
    const stuck = ow.newWorker({ leaseDurationMs: 1000 });
    // The first worker sends, then dies before recording the send step: its
    // step completion never returns and its heartbeat stops.
    const died = Promise.withResolvers<never>();
    vi.spyOn(backend, "completeStepAttempt").mockReturnValueOnce(died.promise);
    vi.spyOn(backend, "extendWorkflowRunLease").mockRejectedValue(
      new Error("worker died"),
    );
    try {
      // created first, so it is claimed first
      const { workflowRun } = await sender.run({ to: "target" });
      await receiver.run(undefined, { id: "target" });
      expect(await stuck.tick()).toBe(1);
      await vi.waitFor(async () => {
        expect(await messages(backend, "target")).toHaveLength(1);
      });

      vi.setSystemTime(new Date(T0.getTime() + 2000));
      await tickUntil(worker, backend, workflowRun.id, {
        ...parked,
        attempts: 2,
      });
      vi.setSystemTime(new Date(T0.getTime() + 2 * HOUR));
      await tickUntil(worker, backend, workflowRun.id, { status: "completed" });
      await tickUntil(worker, backend, "target", {
        status: "completed",
        output: { data: { from: "sender" } },
      });

      const inbox = await messages(backend, "target");
      expect(inbox).toHaveLength(1);
      expect(
        await backend.getWorkflowRun({ workflowRunId: workflowRun.id }),
      ).toMatchObject({ output: { messageId: inbox[0]?.id } });
    } finally {
      died.reject(new Error("worker died"));
      await stuck.stop();
      await worker.stop();
      await backend.stop();
    }
  });

  describe("sendMessageSync", () => {
    test("commits and rolls back with the caller's transaction", async () => {
      const db = newDatabase(":memory:");
      const backend = BackendSqlite.fromDatabase(db);
      const { worker } = approvalClient(backend);
      const createAndSend = () => {
        backend.createWorkflowRunSync(runParams({ id: "event:1" }));
        backend.sendMessageSync({
          workflowRunId: "event:1",
          name: "approval",
          data: "with start",
          idempotencyKey: null,
        });
      };

      try {
        db.exec("BEGIN IMMEDIATE");
        createAndSend();
        db.exec("ROLLBACK");
        expect(count(db, "workflow_runs")).toBe(0);
        expect(count(db, "workflow_messages")).toBe(0);

        db.exec("BEGIN IMMEDIATE");
        createAndSend();
        db.exec("COMMIT");
        expect(count(db, "workflow_messages")).toBe(1);

        await tickUntil(worker, backend, "event:1", {
          status: "completed",
          output: { data: "with start" },
        });
        expect(() =>
          backend.sendMessageSync({
            workflowRunId: "missing",
            name: "approval",
            data: null,
            idempotencyKey: null,
          }),
        ).toThrow(/does not exist/);
      } finally {
        await worker.stop();
        db.close();
      }
    });
  });

  test("an existing database upgrades in place", () => {
    const db = newDatabase(":memory:");
    try {
      for (const sql of migrations()) db.exec(sql);
      db.exec(forkMigrations()[0] ?? "");
      db.prepare(
        `INSERT INTO "workflow_runs" (
          "namespace_id", "id", "workflow_name", "status", "config",
          "attempts", "available_at", "created_at", "updated_at"
        )
        VALUES ('default', 'existing', 'approval', 'pending', '{}', 0, ?, ?, ?)`,
      ).run(T0.toISOString(), T0.toISOString(), T0.toISOString());

      migrate(db);
      migrate(db);

      expect(count(db, "openworkflow_fork_migrations")).toBe(
        forkMigrations().length,
      );
      const backend = BackendSqlite.fromDatabase(db, { runMigrations: false });
      backend.sendMessageSync({
        workflowRunId: "existing",
        name: "approval",
        data: null,
        idempotencyKey: null,
      });
      expect(count(db, "workflow_messages")).toBe(1);
    } finally {
      db.close();
    }
  });
});
