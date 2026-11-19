import { OpenWorkflow } from "../client/client.js";
import type { Logger } from "../core/logger.js";
import type { WorkflowRun } from "../core/workflow-run.js";
import type { Worker } from "../worker/worker.js";
import { BackendSqlite } from "./backend.js";
import { afterEach, describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

const T0 = new Date("2026-01-02T12:00:00.000Z");
const HOUR = 60 * 60 * 1000;

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

async function runsOf(backend: BackendSqlite, workflowName: string) {
  const page = await backend.listWorkflowRuns({ workflowName });
  return page.data;
}

describe("keys and ids on child runs (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("a keyless parent waits on a keyed child that queues behind its key", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const order: string[] = [];
    const blockerGate = Promise.withResolvers<void>();
    const blocker = ow.defineWorkflow({ name: "blocker" }, async () => {
      await blockerGate.promise;
      order.push("blocker");
    });
    const draft = ow.defineWorkflow<{ n: number }, number>(
      { name: "draft" },
      ({ input }) => {
        order.push("draft");
        return input.n * 2;
      },
    );
    const parent = ow.defineWorkflow({ name: "parent" }, async ({ step }) => {
      const doubled = await step.runWorkflow(
        draft.workflow.spec,
        { n: 21 },
        { concurrencyKey: "company:acme" },
      );
      order.push("parent");
      return doubled;
    });
    const worker = ow.newWorker({ concurrency: 3 });
    try {
      await blocker.run(undefined, { concurrencyKey: "company:acme" });
      const { workflowRun } = await parent.run();
      await tickUntil(worker, backend, workflowRun.id, parked);

      const [child] = await runsOf(backend, "draft");
      expect(child).toMatchObject({ status: "pending", attempts: 0 });
      expect(child?.parentStepAttemptId).not.toBeNull();
      expect(await worker.tick()).toBe(0);

      blockerGate.resolve();
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        output: 42,
      });
      expect(order).toEqual(["blocker", "draft", "parent"]);
    } finally {
      blockerGate.resolve();
      await worker.stop();
      await backend.stop();
    }
  });

  test("a child with its parent's key is refused without retry", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const draft = ow.defineWorkflow({ name: "draft" }, () => null);
    const parent = ow.defineWorkflow({ name: "parent" }, async ({ step }) => {
      await step.runWorkflow(draft.workflow.spec, undefined, {
        concurrencyKey: "company:acme",
      });
    });
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await parent.run(undefined, {
        concurrencyKey: "company:acme",
      });
      await tickUntil(worker, backend, workflowRun.id, { status: "failed" });

      const failed = await backend.getWorkflowRun({
        workflowRunId: workflowRun.id,
      });
      expect(failed?.attempts).toBe(1);
      expect(failed?.error?.message).toMatch(
        /cannot use its parent's concurrency key "company:acme"/,
      );
      expect(await runsOf(backend, "draft")).toEqual([]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a parent given an existing child id waits on that run", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const draft = ow.defineWorkflow<{ n: number }, number>(
      { name: "draft" },
      ({ input }) => input.n,
    );
    const parent = ow.defineWorkflow({ name: "parent" }, async ({ step }) => {
      return await step.runWorkflow(
        draft.workflow.spec,
        { n: 2 },
        { id: "draft:1", concurrencyKey: "company:acme" },
      );
    });
    const worker = ow.newWorker();
    try {
      // created by the application, not by the parent, and due in an hour
      await draft.run({ n: 1 }, { id: "draft:1", availableAt: "1h" });
      const { workflowRun } = await parent.run();
      await tickUntil(worker, backend, workflowRun.id, parked);

      expect(await runsOf(backend, "draft")).toHaveLength(1);
      const steps = await backend.listStepAttempts({
        workflowRunId: workflowRun.id,
      });
      expect(steps.data[0]?.childWorkflowRunId).toBe("draft:1");

      // the child finishing wakes the parent, though it did not create it
      vi.setSystemTime(new Date(T0.getTime() + HOUR));
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        output: 1,
      });
      expect(await runsOf(backend, "draft")).toHaveLength(1);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a parent crash and replay does not create a second child", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const draft = ow.defineWorkflow({ name: "draft" }, () => "drafted");
    const parent = ow.defineWorkflow(
      { name: "parent" },
      async ({ step }) =>
        await step.runWorkflow(draft.workflow.spec, undefined, {
          concurrencyKey: "company:acme",
        }),
    );
    const worker = ow.newWorker();
    const stuck = ow.newWorker({ leaseDurationMs: 1000 });
    // The first worker creates the child, then dies before linking it to the
    // parent's step: the link never returns and its heartbeat stops.
    const died = Promise.withResolvers<never>();
    vi.spyOn(backend, "setStepAttemptChildWorkflowRun").mockReturnValueOnce(
      died.promise,
    );
    vi.spyOn(backend, "extendWorkflowRunLease").mockRejectedValue(
      new Error("worker died"),
    );
    try {
      const { workflowRun } = await parent.run();
      expect(await stuck.tick()).toBe(1);
      await vi.waitFor(async () => {
        expect(await runsOf(backend, "draft")).toHaveLength(1);
      });

      vi.setSystemTime(new Date(T0.getTime() + 2000));
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        attempts: 2,
        output: "drafted",
      });
      expect(await runsOf(backend, "draft")).toHaveLength(1);
    } finally {
      died.reject(new Error("worker died"));
      await stuck.stop();
      await worker.stop();
      await backend.stop();
    }
  });
});
