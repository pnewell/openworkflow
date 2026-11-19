import { OpenWorkflow } from "../client/client.js";
import type { Logger } from "../core/logger.js";
import type { WorkflowRun } from "../core/workflow-run.js";
import type { Worker } from "../worker/worker.js";
import { BackendSqlite } from "./backend.js";
import { forkMigrations, migrate, migrations, newDatabase } from "./sqlite.js";
import { afterEach, describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

// hourly ticks fall at 13:00, 14:00, 15:00 after T0
const T0 = new Date("2026-01-02T12:00:00.000Z");
const cron = "0 * * * *";

function at(time: string): Date {
  return new Date(`2026-01-02T${time}.000Z`);
}

/**
 * Process due ticks as the worker's schedule loop does, at the given time.
 * @param backend - Backend holding the schedules
 * @param time - Wall-clock time of the dispatch
 * @returns Number of due schedules processed
 */
async function dispatchAt(
  backend: BackendSqlite,
  time: string,
): Promise<number> {
  vi.setSystemTime(at(time));
  return await backend.enqueueScheduledRuns({ before: at(time), limit: 100 });
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

async function scheduledRuns(backend: BackendSqlite) {
  const page = await backend.listWorkflowRuns({ workflowName: "report" });
  return page.data.map((run) => run.id).toSorted((a, b) => a.localeCompare(b));
}

describe("schedule keys and overlap (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("a keyed scheduled run waits behind another run with its key", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const order: string[] = [];
    const blockerGate = Promise.withResolvers<void>();
    const blocker = ow.defineWorkflow({ name: "blocker" }, async () => {
      await blockerGate.promise;
      order.push("blocker");
    });
    const report = ow.defineWorkflow({ name: "report" }, () => {
      order.push("report");
    });
    const worker = ow.newWorker({ concurrency: 2 });
    try {
      await report.schedule(undefined, {
        cron,
        concurrencyKey: "company:acme",
      });
      // start the blocker at the tick's time, so its lease is current
      vi.setSystemTime(at("13:00:01"));
      const { workflowRun } = await blocker.run(undefined, {
        concurrencyKey: "company:acme",
      });
      await tickUntil(worker, backend, workflowRun.id, { status: "running" });

      expect(await dispatchAt(backend, "13:00:01")).toBe(1);
      const [runId] = await scheduledRuns(backend);
      expect(await worker.tick()).toBe(0);
      expect(
        await backend.getWorkflowRun({ workflowRunId: runId ?? "" }),
      ).toMatchObject({ status: "pending", attempts: 0 });

      blockerGate.resolve();
      await tickUntil(worker, backend, runId ?? "", { status: "completed" });
      expect(order).toEqual(["blocker", "report"]);
    } finally {
      blockerGate.resolve();
      await worker.stop();
      await backend.stop();
    }
  });

  test("skip creates nothing for a tick while an earlier run is unfinished", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const report = ow.defineWorkflow({ name: "report" }, () => null);
    const worker = ow.newWorker();
    try {
      await report.schedule(undefined, { cron, overlap: "skip" });

      expect(await dispatchAt(backend, "13:00:01")).toBe(1);
      // the 13:00 run has not run yet, so the 14:00 tick is skipped
      expect(await dispatchAt(backend, "14:00:01")).toBe(1);
      expect(await scheduledRuns(backend)).toEqual([
        "__schedule:report:2026-01-02T13:00:00.000Z",
      ]);
      expect(await ow.schedules.get("report")).toMatchObject({
        nextRunAt: at("15:00:00"),
      });

      await tickUntil(
        worker,
        backend,
        "__schedule:report:2026-01-02T13:00:00.000Z",
        { status: "completed" },
      );
      expect(await dispatchAt(backend, "15:00:01")).toBe(1);
      expect(await scheduledRuns(backend)).toEqual([
        "__schedule:report:2026-01-02T13:00:00.000Z",
        "__schedule:report:2026-01-02T15:00:00.000Z",
      ]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("allow still overlaps, as before", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const report = ow.defineWorkflow({ name: "report" }, () => null);
    try {
      await report.schedule(undefined, { cron });

      await dispatchAt(backend, "13:00:01");
      await dispatchAt(backend, "14:00:01");
      expect(await scheduledRuns(backend)).toEqual([
        "__schedule:report:2026-01-02T13:00:00.000Z",
        "__schedule:report:2026-01-02T14:00:00.000Z",
      ]);
    } finally {
      await backend.stop();
    }
  });

  test("the same tick processed twice creates one run", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const report = ow.defineWorkflow({ name: "report" }, () => null);
    try {
      await report.schedule(undefined, { cron });
      await dispatchAt(backend, "13:00:01");

      // as if the cursor advance were lost, the 13:00 tick is due again
      backend["db"]
        .prepare(`UPDATE "workflow_schedules" SET "next_run_at" = ?`)
        .run(at("13:00:00").toISOString());
      expect(await dispatchAt(backend, "13:00:02")).toBe(1);

      expect(await scheduledRuns(backend)).toEqual([
        "__schedule:report:2026-01-02T13:00:00.000Z",
      ]);
    } finally {
      await backend.stop();
    }
  });

  test("the options survive pause, resume and re-declaring, by either route", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const options = {
      cron,
      concurrencyKey: "company:acme",
      overlap: "skip" as const,
    };
    const report = ow.defineWorkflow(
      { name: "report", schedules: [options] },
      () => null,
    );
    const worker = ow.newWorker();
    const kept = { concurrencyKey: "company:acme", overlap: "skip" };
    try {
      // declared in the workflow's definition, persisted when a worker starts
      await worker.start();
      await worker.stop();
      expect(await ow.schedules.get("report")).toMatchObject(kept);

      expect(await ow.schedules.pause("report")).toMatchObject(kept);
      expect(await ow.schedules.resume("report")).toMatchObject(kept);
      expect(await report.schedule(undefined, options)).toMatchObject(kept);

      await dispatchAt(backend, "13:00:01");
      expect(
        backend["db"]
          .prepare(`SELECT "concurrency_key" FROM "workflow_runs"`)
          .all(),
      ).toEqual([{ concurrency_key: "company:acme" }]);

      // re-declaring without them is a new declaration: last write wins
      expect(await report.schedule(undefined, { cron })).toMatchObject({
        concurrencyKey: null,
        overlap: "allow",
      });
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  test("an existing database upgrades in place", async () => {
    const db = newDatabase(":memory:");
    try {
      for (const sql of migrations()) db.exec(sql);
      for (const sql of forkMigrations().slice(0, 2)) db.exec(sql);
      db.prepare(
        `INSERT INTO "workflow_schedules" (
          "namespace_id", "id", "workflow_name", "cron", "timezone",
          "next_run_at", "created_at", "updated_at"
        )
        VALUES ('default', 'report', 'report', ?, 'UTC', ?, ?, ?)`,
      ).run(
        cron,
        at("13:00:00").toISOString(),
        T0.toISOString(),
        T0.toISOString(),
      );

      migrate(db);
      migrate(db);

      const backend = BackendSqlite.fromDatabase(db, { runMigrations: false });
      expect(
        await backend.getWorkflowSchedule({ workflowScheduleId: "report" }),
      ).toMatchObject({ concurrencyKey: null, overlap: "allow" });
      expect(
        await backend.enqueueScheduledRuns({
          before: at("13:00:01"),
          limit: 1,
        }),
      ).toBe(1);
    } finally {
      db.close();
    }
  });
});
