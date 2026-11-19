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

async function runsOf(backend: BackendSqlite, workflowName: string) {
  const page = await backend.listWorkflowRuns({ workflowName });
  return page.data;
}

describe("step.startWorkflow (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("a replay returns the same id and one run exists", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const task = ow.defineWorkflow<{ n: number }, number>(
      { name: "task" },
      ({ input }) => input.n,
    );
    const starter = ow.defineWorkflow({ name: "starter" }, async ({ step }) => {
      const id = await step.startWorkflow(task.workflow.spec, { n: 1 });
      await step.sleep("pause", "1h");
      return id;
    });
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await starter.run();
      await tickUntil(worker, backend, workflowRun.id, {
        status: "running",
        workerId: null,
      });
      vi.setSystemTime(new Date(T0.getTime() + HOUR));
      await tickUntil(worker, backend, workflowRun.id, { status: "completed" });

      const tasks = await runsOf(backend, "task");
      expect(tasks).toHaveLength(1);
      const finished = await backend.getWorkflowRun({
        workflowRunId: workflowRun.id,
      });
      expect(finished?.output).toBe(tasks[0]?.id);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a crash between creating the run and recording the step starts one run", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const task = ow.defineWorkflow({ name: "task" }, () => "done");
    const starter = ow.defineWorkflow(
      { name: "starter" },
      async ({ step }) => await step.startWorkflow(task.workflow.spec),
    );
    const worker = ow.newWorker();
    const stuck = ow.newWorker({ leaseDurationMs: 1000 });
    // The first worker creates the run, then dies before recording the step:
    // the step completion never returns and its heartbeat stops.
    const died = Promise.withResolvers<never>();
    vi.spyOn(backend, "completeStepAttempt").mockReturnValueOnce(died.promise);
    vi.spyOn(backend, "extendWorkflowRunLease").mockRejectedValue(
      new Error("worker died"),
    );
    try {
      const { workflowRun } = await starter.run();
      expect(await stuck.tick()).toBe(1);
      await vi.waitFor(async () => {
        expect(await runsOf(backend, "task")).toHaveLength(1);
      });

      vi.setSystemTime(new Date(T0.getTime() + 2000));
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        attempts: 2,
      });
      const tasks = await runsOf(backend, "task");
      expect(tasks).toHaveLength(1);
      const finished = await backend.getWorkflowRun({
        workflowRunId: workflowRun.id,
      });
      expect(finished?.output).toBe(tasks[0]?.id);
    } finally {
      died.reject(new Error("worker died"));
      await stuck.stop();
      await worker.stop();
      await backend.stop();
    }
  });

  test("the starter finishes without waiting, and the started run's failure does not affect it", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const task = ow.defineWorkflow({ name: "task" }, () => {
      throw new Error("task failed");
    });
    const starter = ow.defineWorkflow(
      { name: "starter" },
      async ({ step }) =>
        await step.startWorkflow(task.workflow.spec, undefined, {
          availableAt: "1h",
          id: "task:1",
        }),
    );
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await starter.run();
      await tickUntil(worker, backend, workflowRun.id, {
        status: "completed",
        output: "task:1",
      });
      const started = await backend.getWorkflowRun({ workflowRunId: "task:1" });
      expect(started).toMatchObject({
        status: "pending",
        attempts: 0,
        parentStepAttemptId: null,
      });
      expect(started?.availableAt?.getTime()).toBeGreaterThanOrEqual(
        T0.getTime() + HOUR,
      );
      expect(await worker.tick()).toBe(0);

      vi.setSystemTime(new Date(T0.getTime() + 2 * HOUR));
      await tickUntil(worker, backend, "task:1", { status: "failed" });
      expect(
        await backend.getWorkflowRun({ workflowRunId: workflowRun.id }),
      ).toMatchObject({ status: "completed", output: "task:1" });
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("a keyed start queues behind its key, including the starter's own", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const order: string[] = [];
    const starterGate = Promise.withResolvers<void>();
    const task = ow.defineWorkflow({ name: "task" }, () => {
      order.push("task");
    });
    const starter = ow.defineWorkflow({ name: "starter" }, async ({ step }) => {
      const id = await step.startWorkflow(task.workflow.spec, undefined, {
        concurrencyKey: "company:acme",
      });
      await step.run({ name: "finish" }, async () => {
        await starterGate.promise;
        order.push("starter");
      });
      return id;
    });
    const worker = ow.newWorker({ concurrency: 2 });
    try {
      const { workflowRun } = await starter.run(undefined, {
        concurrencyKey: "company:acme",
      });
      await vi.waitFor(async () => {
        await worker.tick();
        expect(await runsOf(backend, "task")).toHaveLength(1);
      });
      expect(await worker.tick()).toBe(0);
      const [started] = await runsOf(backend, "task");
      expect(started).toMatchObject({ status: "pending", attempts: 0 });

      starterGate.resolve();
      await tickUntil(worker, backend, workflowRun.id, { status: "completed" });
      await tickUntil(worker, backend, started?.id ?? "", {
        status: "completed",
      });
      expect(order).toEqual(["starter", "task"]);
    } finally {
      starterGate.resolve();
      await worker.stop();
      await backend.stop();
    }
  });
});
