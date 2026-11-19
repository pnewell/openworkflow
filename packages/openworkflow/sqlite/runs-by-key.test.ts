import { OpenWorkflow } from "../client/client.js";
import type { CreateWorkflowRunParams } from "../core/backend.js";
import type { Logger } from "../core/logger.js";
import { BackendSqlite } from "./backend.js";
import { afterEach, describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

const T0 = new Date("2026-01-02T12:00:00.000Z");
const DAY = 24 * 60 * 60 * 1000;

function runParams(
  overrides: Partial<CreateWorkflowRunParams> = {},
): CreateWorkflowRunParams {
  return {
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
    ...overrides,
  };
}

async function statuses(backend: BackendSqlite, ids: readonly string[]) {
  const result: Record<string, string | undefined> = {};
  for (const id of ids) {
    const run = await backend.getWorkflowRun({ workflowRunId: id });
    result[id] = run?.status;
  }
  return result;
}

describe("runs by concurrency key (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("runs expose their key and can be listed by it", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const turn = ow.defineWorkflow({ name: "turn" }, () => null);
    try {
      const a1 = await turn.run(undefined, { id: "a1", concurrencyKey: "a" });
      await turn.run(undefined, { id: "a2", concurrencyKey: "a" });
      await turn.run(undefined, { id: "b1", concurrencyKey: "b" });
      const none = await turn.run(undefined, { id: "none" });

      expect(a1.workflowRun.concurrencyKey).toBe("a");
      expect(none.workflowRun.concurrencyKey).toBeUndefined();
      const page = await backend.listWorkflowRuns({ concurrencyKey: "a" });
      expect(page.data.map((run) => run.id).toSorted()).toEqual(["a1", "a2"]);
      const pending = await backend.listWorkflowRuns({
        concurrencyKey: "b",
        status: "pending",
      });
      expect(pending.data.map((run) => run.id)).toEqual(["b1"]);
    } finally {
      await backend.stop();
    }
  });

  test("cancel by key covers pending, delayed and parked runs, spares one, and frees the key", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const claim = () =>
      backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });
    try {
      backend.createWorkflowRunSync(
        runParams({ id: "parked", concurrencyKey: "acme" }),
      );
      expect(await claim()).toMatchObject({ id: "parked" });
      await backend.sleepWorkflowRun({
        workflowRunId: "parked",
        workerId: "worker-1",
        availableAt: new Date(T0.getTime() + DAY),
      });
      backend.createWorkflowRunSync(
        runParams({ id: "pending", concurrencyKey: "acme" }),
      );
      backend.createWorkflowRunSync(
        runParams({
          id: "delayed",
          concurrencyKey: "acme",
          availableAt: new Date(T0.getTime() + DAY),
        }),
      );
      backend.createWorkflowRunSync(
        runParams({ id: "keep", concurrencyKey: "acme" }),
      );
      backend.createWorkflowRunSync(
        runParams({ id: "other", concurrencyKey: "other" }),
      );
      const ids = ["parked", "pending", "delayed", "keep", "other"];

      const cancel = () =>
        ow.cancelWorkflowRunsByConcurrencyKey("acme", {
          exceptWorkflowRunId: "keep",
        });
      const result = await cancel();
      expect(result.workflowRunIds.toSorted()).toEqual([
        "delayed",
        "parked",
        "pending",
      ]);
      expect(await statuses(backend, ids)).toEqual({
        parked: "canceled",
        pending: "canceled",
        delayed: "canceled",
        keep: "pending",
        other: "pending",
      });

      // the parked run no longer holds the key, so the spared run is claimed
      expect(await claim()).toMatchObject({ id: "keep" });

      expect(await cancel()).toEqual({ workflowRunIds: [] });
      expect(await statuses(backend, ids)).toEqual({
        parked: "canceled",
        pending: "canceled",
        delayed: "canceled",
        keep: "running",
        other: "pending",
      });
    } finally {
      await backend.stop();
    }
  });

  test("a keyed run cancels the rest of its key without canceling itself", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    ow.defineWorkflow({ name: "turn" }, () => null);
    const sweeper = ow.defineWorkflow(
      { name: "sweeper" },
      async ({ step, run }) =>
        await step.run({ name: "sweep" }, async () => {
          const { workflowRunIds } =
            await ow.cancelWorkflowRunsByConcurrencyKey("acme", {
              exceptWorkflowRunId: run.id,
            });
          return workflowRunIds.toSorted();
        }),
    );
    const worker = ow.newWorker();
    try {
      const { workflowRun } = await sweeper.run(undefined, {
        concurrencyKey: "acme",
      });
      for (const id of ["later-1", "later-2"]) {
        backend.createWorkflowRunSync(
          runParams({ id, concurrencyKey: "acme" }),
        );
      }

      await vi.waitFor(
        async () => {
          await worker.tick();
          expect(
            await backend.getWorkflowRun({ workflowRunId: workflowRun.id }),
          ).toMatchObject({
            status: "completed",
            output: ["later-1", "later-2"],
          });
        },
        { timeout: 5000, interval: 5 },
      );
      expect(await statuses(backend, ["later-1", "later-2"])).toEqual({
        "later-1": "canceled",
        "later-2": "canceled",
      });
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });
});
