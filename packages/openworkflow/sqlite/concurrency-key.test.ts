import { OpenWorkflow } from "../client/client.js";
import type { CreateWorkflowRunParams } from "../core/backend.js";
import type { Logger } from "../core/logger.js";
import type { Worker } from "../worker/worker.js";
import { BackendSqlite } from "./backend.js";
import {
  Database,
  forkMigrations,
  migrate,
  migrations,
  newDatabase,
} from "./sqlite.js";
import { randomUUID } from "node:crypto";
import { existsSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout } from "node:timers/promises";
import { afterEach, describe, expect, test, vi } from "vitest";

const quietLogger: Logger = {
  debug: () => null,
  info: () => null,
  warn: () => null,
  error: () => null,
};

const T0 = new Date("2026-01-02T12:00:00.000Z");

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

function count(db: Database, table: string): number {
  // safety: COUNT(*) returns a numeric count column for this query.
  const row = db.prepare(`SELECT COUNT(*) AS "count" FROM "${table}"`).get() as
    { count: number } | undefined;
  return row?.count ?? 0;
}

function migrationVersion(db: Database, table: string): number | null {
  // safety: MAX(version) returns an integer, or null when the table is empty.
  const row = db
    .prepare(`SELECT MAX("version") AS "version" FROM "${table}"`)
    .get() as { version: number | null } | undefined;
  return row?.version ?? null;
}

async function status(
  backend: BackendSqlite,
  workflowRunId: string,
): Promise<string | undefined> {
  const run = await backend.getWorkflowRun({ workflowRunId });
  return run?.status;
}

/**
 * Tick until every given run has finished.
 * @param worker - Worker to tick
 * @param backend - Backend holding the runs
 * @param ids - Workflow run ids that must finish
 */
async function tickUntilFinished(
  worker: Worker,
  backend: BackendSqlite,
  ids: readonly string[],
): Promise<void> {
  await vi.waitFor(
    async () => {
      await worker.tick();
      for (const id of ids) {
        expect(["completed", "failed", "canceled"]).toContain(
          await status(backend, id),
        );
      }
    },
    { timeout: 5000, interval: 5 },
  );
}

function removeDatabaseFiles(dbPath: string): void {
  for (const file of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) {
    if (existsSync(file)) unlinkSync(file);
  }
}

describe("concurrency keys (SQLite)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("createWorkflowRunSync joins the caller's open transaction", () => {
    // node:sqlite DatabaseSync under Node, bun:sqlite Database under Bun
    const db = newDatabase(":memory:");
    const backend = BackendSqlite.fromDatabase(db);
    db.exec(`CREATE TABLE "events" ("id" TEXT NOT NULL PRIMARY KEY)`);
    const insertEvent = db.prepare(`INSERT INTO "events" ("id") VALUES (?)`);

    try {
      db.exec("BEGIN IMMEDIATE");
      insertEvent.run("event-1");
      backend.createWorkflowRunSync(runParams({ id: "event:1" }));
      db.exec("ROLLBACK");
      expect(count(db, "events")).toBe(0);
      expect(count(db, "workflow_runs")).toBe(0);

      db.exec("BEGIN IMMEDIATE");
      insertEvent.run("event-1");
      const run = backend.createWorkflowRunSync(runParams({ id: "event:1" }));
      db.exec("COMMIT");
      expect(run.id).toBe("event:1");
      expect(count(db, "events")).toBe(1);
      expect(count(db, "workflow_runs")).toBe(1);

      expect(() =>
        backend.createWorkflowRunSync(runParams({ idempotencyKey: "key" })),
      ).toThrow(/does not accept an idempotencyKey/);
    } finally {
      db.close();
    }
  });

  test("creating a run with an existing id returns the existing run", async () => {
    const backend = BackendSqlite.connect(":memory:");
    try {
      const created = backend.createWorkflowRunSync(
        runParams({ id: "event:1", input: { n: 1 } }),
      );
      const claimed = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });
      expect(claimed?.id).toBe(created.id);
      const completed = await backend.completeWorkflowRun({
        workflowRunId: created.id,
        workerId: "worker-1",
        output: null,
      });

      const repeat = backend.createWorkflowRunSync(
        runParams({ id: "event:1", workflowName: "other", input: { n: 2 } }),
      );
      const repeatAsync = await backend.createWorkflowRun(
        runParams({ id: "event:1", concurrencyKey: "company:1" }),
      );

      expect(repeat).toEqual(completed);
      expect(repeatAsync).toEqual(completed);
      expect(count(backend["db"], "workflow_runs")).toBe(1);
    } finally {
      await backend.stop();
    }
  });

  test("runs sharing a key start in order and never overlap, while keys interleave", async () => {
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const names = ["a1", "a2", "a3", "b1", "b2"];
    const started: string[] = [];
    const gates = new Map(
      names.map((name) => [name, Promise.withResolvers<void>()]),
    );
    const workflow = ow.defineWorkflow<{ name: string }, void>(
      { name: "turn" },
      async ({ input }) => {
        started.push(input.name);
        await gates.get(input.name)?.promise;
      },
    );
    const worker = ow.newWorker({ concurrency: 4 });
    const tickAndExpectStarted = async (claimed: number, all: string[]) => {
      expect(await worker.tick()).toBe(claimed);
      await vi.waitFor(() => {
        expect(started).toEqual(all);
      });
    };
    const finish = async (name: string) => {
      gates.get(name)?.resolve();
      await vi.waitFor(async () => {
        expect(await status(backend, name)).toBe("completed");
      });
    };

    try {
      for (const name of names) {
        await workflow.run(
          { name },
          { id: name, concurrencyKey: `company:${name.charAt(0)}` },
        );
      }

      await tickAndExpectStarted(2, ["a1", "b1"]);
      expect(await worker.tick()).toBe(0);

      await finish("a1");
      await tickAndExpectStarted(1, ["a1", "b1", "a2"]);
      await finish("b1");
      await tickAndExpectStarted(1, ["a1", "b1", "a2", "b2"]);
      await finish("a2");
      await tickAndExpectStarted(1, ["a1", "b1", "a2", "b2", "a3"]);
      expect(await worker.tick()).toBe(0);

      await finish("b2");
      await finish("a3");
    } finally {
      for (const gate of gates.values()) gate.resolve();
      await worker.stop();
      await backend.stop();
    }
  });

  test("runs created in the same millisecond are claimed in insertion order", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const db = newDatabase(":memory:");
    const backend = BackendSqlite.fromDatabase(db);
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const seen: number[] = [];
    ow.defineWorkflow<{ n: number }, void>({ name: "turn" }, ({ input }) => {
      seen.push(input.n);
    });
    const worker = ow.newWorker();
    // ids sort in the reverse of insertion order
    const ids = Array.from(
      { length: 10 },
      (_, n) => `run-${String(99 - n).padStart(2, "0")}`,
    );

    try {
      db.exec("BEGIN IMMEDIATE");
      for (const [n, id] of ids.entries()) {
        backend.createWorkflowRunSync(
          runParams({ id, concurrencyKey: "company:1", input: { n } }),
        );
      }
      db.exec("COMMIT");

      await tickUntilFinished(worker, backend, ids);
      expect(seen).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    } finally {
      await worker.stop();
      await backend.stop();
      db.close();
    }
  });

  test("a delayed run does not block an immediate one and is claimed once due", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    const ow = new OpenWorkflow({ backend, logger: quietLogger });
    const seen: string[] = [];
    ow.defineWorkflow<{ what: string }, void>({ name: "turn" }, ({ input }) => {
      seen.push(input.what);
    });
    const worker = ow.newWorker();

    try {
      backend.createWorkflowRunSync(
        runParams({
          id: "follow-up",
          concurrencyKey: "company:1",
          input: { what: "follow-up" },
          availableAt: new Date(T0.getTime() + 7 * 24 * 60 * 60 * 1000),
        }),
      );
      backend.createWorkflowRunSync(
        runParams({
          id: "mail",
          concurrencyKey: "company:1",
          input: { what: "mail" },
        }),
      );

      await tickUntilFinished(worker, backend, ["mail"]);
      expect(await worker.tick()).toBe(0);
      expect(seen).toEqual(["mail"]);
      expect(
        await backend.getWorkflowRun({ workflowRunId: "follow-up" }),
      ).toMatchObject({ status: "pending", attempts: 0 });

      vi.setSystemTime(new Date(T0.getTime() + 7 * 24 * 60 * 60 * 1000 + 1));
      await tickUntilFinished(worker, backend, ["follow-up"]);
      expect(seen).toEqual(["mail", "follow-up"]);
    } finally {
      await worker.stop();
      await backend.stop();
    }
  });

  describe("a started run holds its key until it finishes", () => {
    test("while waiting out a step retry, released by permanent failure", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(T0);
      const backend = BackendSqlite.connect(":memory:");
      const ow = new OpenWorkflow({ backend, logger: quietLogger });
      const seen: string[] = [];
      ow.defineWorkflow<{ what: string }, void>(
        { name: "turn" },
        async ({ input, step }) => {
          await step.run(
            {
              name: "work",
              retryPolicy: { maximumAttempts: 2, initialInterval: "1m" },
            },
            () => {
              seen.push(input.what);
              if (input.what === "poison") throw new Error("always fails");
            },
          );
        },
      );
      const worker = ow.newWorker({ concurrency: 2 });

      try {
        for (const what of ["poison", "next"]) {
          backend.createWorkflowRunSync(
            runParams({
              id: what,
              concurrencyKey: "company:1",
              input: { what },
            }),
          );
        }

        await vi.waitFor(async () => {
          await worker.tick();
          expect(
            await backend.getWorkflowRun({ workflowRunId: "poison" }),
          ).toMatchObject({ status: "pending", attempts: 1, startedAt: null });
        });
        expect(await worker.tick()).toBe(0);
        expect(seen).toEqual(["poison"]);
        expect(await status(backend, "next")).toBe("pending");

        vi.setSystemTime(new Date(T0.getTime() + 2 * 60 * 1000));
        await tickUntilFinished(worker, backend, ["poison", "next"]);
        expect(seen).toEqual(["poison", "poison", "next"]);
        expect(await status(backend, "poison")).toBe("failed");
        expect(await status(backend, "next")).toBe("completed");
      } finally {
        await worker.stop();
        await backend.stop();
      }
    });

    test("while parked, released by cancellation", async () => {
      const backend = BackendSqlite.connect(":memory:");
      try {
        backend.createWorkflowRunSync(
          runParams({ id: "first", concurrencyKey: "company:1" }),
        );
        backend.createWorkflowRunSync(
          runParams({ id: "second", concurrencyKey: "company:1" }),
        );
        const claim = () =>
          backend.claimWorkflowRun({
            workerId: "worker-1",
            leaseDurationMs: 30_000,
          });

        expect(await claim()).toMatchObject({ id: "first" });
        await backend.sleepWorkflowRun({
          workflowRunId: "first",
          workerId: "worker-1",
          availableAt: new Date(Date.now() + 60 * 60 * 1000),
        });
        expect(await claim()).toBeNull();

        await backend.cancelWorkflowRun({ workflowRunId: "first" });
        expect(await claim()).toMatchObject({ id: "second" });
      } finally {
        await backend.stop();
      }
    });

    test("released when its deadline passes", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(T0);
      const backend = BackendSqlite.connect(":memory:");
      try {
        backend.createWorkflowRunSync(
          runParams({
            id: "first",
            concurrencyKey: "company:1",
            deadlineAt: new Date(T0.getTime() + 60 * 1000),
          }),
        );
        backend.createWorkflowRunSync(
          runParams({ id: "second", concurrencyKey: "company:1" }),
        );
        const claim = () =>
          backend.claimWorkflowRun({
            workerId: "worker-1",
            leaseDurationMs: 5 * 60 * 1000,
          });

        expect(await claim()).toMatchObject({ id: "first" });
        expect(await claim()).toBeNull();

        vi.setSystemTime(new Date(T0.getTime() + 2 * 60 * 1000));
        expect(await claim()).toMatchObject({ id: "second" });
        expect(await status(backend, "first")).toBe("failed");
      } finally {
        await backend.stop();
      }
    });
  });

  test("a run whose lease expired is reclaimed while its key's other runs wait", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    const backend = BackendSqlite.connect(":memory:");
    try {
      backend.createWorkflowRunSync(
        runParams({ id: "first", concurrencyKey: "company:1" }),
      );
      backend.createWorkflowRunSync(
        runParams({ id: "second", concurrencyKey: "company:1" }),
      );

      const first = await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 1000,
      });
      expect(first?.id).toBe("first");

      vi.setSystemTime(new Date(T0.getTime() + 2000));
      const reclaimed = await backend.claimWorkflowRun({
        workerId: "worker-2",
        leaseDurationMs: 1000,
      });
      expect(reclaimed).toMatchObject({
        id: "first",
        attempts: 2,
        workerId: "worker-2",
      });

      await backend.completeWorkflowRun({
        workflowRunId: "first",
        workerId: "worker-2",
        output: null,
      });
      const second = await backend.claimWorkflowRun({
        workerId: "worker-2",
        leaseDurationMs: 1000,
      });
      expect(second?.id).toBe("second");
    } finally {
      await backend.stop();
    }
  });

  test("the database refuses a second started run for a key", async () => {
    const backend = BackendSqlite.connect(":memory:");
    try {
      backend.createWorkflowRunSync(
        runParams({ id: "first", concurrencyKey: "company:1" }),
      );
      backend.createWorkflowRunSync(
        runParams({ id: "second", concurrencyKey: "company:1" }),
      );
      await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });

      expect(() =>
        backend["db"]
          .prepare(
            `UPDATE "workflow_runs" SET "status" = 'running', "attempts" = 1 WHERE "id" = ?`,
          )
          .run("second"),
      ).toThrow(/UNIQUE constraint failed/);
    } finally {
      await backend.stop();
    }
  });

  test("two backends on one file never run two runs of a key at once", async () => {
    const dbPath = path.join(
      tmpdir(),
      `openworkflow-concurrency-key-${randomUUID()}.db`,
    );
    const backendA = BackendSqlite.connect(dbPath);
    const backendB = BackendSqlite.connect(dbPath);
    const backends = [backendA, backendB];
    let active = 0;
    let maxActive = 0;
    const workers = backends.map((backend) => {
      const ow = new OpenWorkflow({ backend, logger: quietLogger });
      ow.defineWorkflow({ name: "turn" }, async ({ step }) => {
        await step.run({ name: "work" }, async () => {
          active++;
          maxActive = Math.max(maxActive, active);
          await setTimeout(2);
          active--;
        });
      });
      return ow.newWorker({ concurrency: 3 });
    });
    const ids = Array.from({ length: 20 }, (_, n) => `run-${String(n)}`);

    try {
      for (const [n, id] of ids.entries()) {
        (n % 2 === 0 ? backendA : backendB).createWorkflowRunSync(
          runParams({ id, concurrencyKey: "company:1" }),
        );
      }

      await vi.waitFor(
        async () => {
          await Promise.all(workers.map((worker) => worker.tick()));
          for (const id of ids) {
            expect(await status(backendA, id)).toBe("completed");
          }
        },
        { timeout: 10_000, interval: 5 },
      );
      expect(maxActive).toBe(1);
    } finally {
      await Promise.all(workers.map((worker) => worker.stop()));
      await Promise.all(backends.map((backend) => backend.stop()));
      removeDatabaseFiles(dbPath);
    }
  });

  test("a rerun keeps its source run's key", async () => {
    const backend = BackendSqlite.connect(":memory:");
    try {
      backend.createWorkflowRunSync(
        runParams({ id: "first", concurrencyKey: "company:1" }),
      );
      await backend.claimWorkflowRun({
        workerId: "worker-1",
        leaseDurationMs: 30_000,
      });
      await backend.completeWorkflowRun({
        workflowRunId: "first",
        workerId: "worker-1",
        output: null,
      });

      const rerun = await backend.rerunWorkflowRun({
        workflowRunId: "first",
        fromStep: null,
        context: null,
      });

      expect(
        backend["db"]
          .prepare(
            `SELECT "concurrency_key" FROM "workflow_runs" WHERE "id" = ?`,
          )
          .get(rerun.id),
      ).toEqual({ concurrency_key: "company:1" });
      backend.createWorkflowRunSync(
        runParams({ id: "second", concurrencyKey: "company:1" }),
      );
      const claim = () =>
        backend.claimWorkflowRun({
          workerId: "worker-1",
          leaseDurationMs: 30_000,
        });
      expect(await claim()).toMatchObject({ id: rerun.id });
      expect(await claim()).toBeNull();
    } finally {
      await backend.stop();
    }
  });

  describe("BackendSqlite.fromDatabase", () => {
    test("runs migrations and leaves the connection open on stop", async () => {
      const db = newDatabase(":memory:");
      try {
        const backend = BackendSqlite.fromDatabase(db);
        expect(migrationVersion(db, "openworkflow_migrations")).toBe(
          migrations().length - 1,
        );
        expect(migrationVersion(db, "openworkflow_fork_migrations")).toBe(
          forkMigrations().length - 1,
        );

        await backend.stop();
        expect(count(db, "workflow_runs")).toBe(0);
      } finally {
        db.close();
      }
    });

    test("skips migrations when runMigrations is false", () => {
      const db = newDatabase(":memory:");
      try {
        BackendSqlite.fromDatabase(db, { runMigrations: false });
        expect(count(db, "sqlite_master")).toBe(0);
      } finally {
        db.close();
      }
    });

    test("connect still closes its own connection on stop", async () => {
      const backend = BackendSqlite.connect(":memory:");
      const db = backend["db"];
      await backend.stop();
      expect(() => db.prepare("SELECT 1")).toThrow();
    });
  });

  describe("fork migrations", () => {
    test("upgrade a database at the latest upstream version in place", () => {
      const db = newDatabase(":memory:");
      try {
        for (const sql of migrations()) db.exec(sql);
        db.prepare(
          `INSERT INTO "workflow_runs" (
            "namespace_id", "id", "workflow_name", "status", "config",
            "attempts", "available_at", "created_at", "updated_at"
          )
          VALUES ('default', 'existing', 'turn', 'pending', '{}', 0, ?, ?, ?)`,
        ).run(T0.toISOString(), T0.toISOString(), T0.toISOString());
        expect(migrationVersion(db, "openworkflow_migrations")).toBe(7);

        migrate(db);
        migrate(db);

        expect(migrationVersion(db, "openworkflow_migrations")).toBe(7);
        expect(count(db, "openworkflow_fork_migrations")).toBe(1);
        expect(migrationVersion(db, "openworkflow_fork_migrations")).toBe(0);
        expect(
          db
            .prepare(
              `SELECT "id", "concurrency_key" FROM "workflow_runs" WHERE "id" = ?`,
            )
            .get("existing"),
        ).toEqual({ id: "existing", concurrency_key: null });
      } finally {
        db.close();
      }
    });

    test("still apply an upstream migration added after them", () => {
      const db = newDatabase(":memory:");
      try {
        // upstream before migration 7, with the fork migrations already applied
        for (const sql of migrations().slice(0, 7)) db.exec(sql);
        for (const sql of forkMigrations()) db.exec(sql);

        migrate(db);

        expect(migrationVersion(db, "openworkflow_migrations")).toBe(7);
        expect(count(db, "workflow_schedules")).toBe(0);
        expect(count(db, "openworkflow_fork_migrations")).toBe(1);
      } finally {
        db.close();
      }
    });
  });
});
