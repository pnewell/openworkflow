import {
  WorkflowRunCounts,
  DEFAULT_NAMESPACE_ID,
  DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS,
  Backend,
  CancelWorkflowRunParams,
  type CancelWorkflowRunsByConcurrencyKeyParams,
  type CancelWorkflowRunsResult,
  ClaimWorkflowRunParams,
  CreateStepAttemptParams,
  CreateWorkflowRunParams,
  RerunWorkflowRunParams,
  GetStepAttemptParams,
  GetWorkflowRunParams,
  ExtendWorkflowRunLeaseParams,
  ListStepAttemptsParams,
  ListWorkflowRunsParams,
  PaginatedResponse,
  FailStepAttemptParams,
  CompleteStepAttemptParams,
  SetStepAttemptChildWorkflowRunParams,
  FailWorkflowRunParams,
  RescheduleWorkflowRunAfterFailedStepAttemptParams,
  CompleteWorkflowRunParams,
  SleepWorkflowRunParams,
  SendSignalParams,
  SendSignalResult,
  GetSignalDeliveryParams,
  toWorkflowRunCounts,
  type ConsumeMessageParams,
  type EnqueueScheduledRunsParams,
  type ListMessagesParams,
  type SendMessageParams,
  type SendMessageResult,
  type WorkflowMessage,
  type PaginationOptions,
  type WorkflowScheduleIdParams,
} from "../core/backend.js";
import {
  buildPaginatedResponse,
  DEFAULT_PAGINATION_PAGE_SIZE,
  decodeListCursor,
} from "../core/cursor.js";
import { requireRow, wrapError } from "../core/error.js";
import { JsonValue } from "../core/json.js";
import { prepareWorkflowRerun } from "../core/rerun.js";
import {
  nextScheduleOccurrence,
  type WorkflowSchedule,
  type WorkflowScheduleDefinition,
  type WorkflowScheduleOverlap,
} from "../core/schedule.js";
import { StepAttempt } from "../core/step-attempt.js";
import { computeFailedWorkflowRunUpdate } from "../core/workflow-definition.js";
import {
  isTerminalStatus,
  resolveCancelWorkflowRunConflict,
  WorkflowRun,
} from "../core/workflow-run.js";
import {
  newDatabase,
  Database,
  migrate,
  generateUUID,
  now,
  addMilliseconds,
  toJSON,
  fromJSON,
  toISO,
  fromISO,
} from "./sqlite.js";
import type { SQLOutputValue } from "node:sqlite";

interface BackendSqliteOptions {
  namespaceId?: string;
  runMigrations?: boolean;
}

/**
 * WHERE fragment matching a running workflow run currently owned by the given
 * worker. Consumes 3 positional placeholders: `namespace_id`, `id`,
 * `worker_id`. Pair with {@link BackendSqlite#runningWorkflowRunOwnedParams}
 * to keep the placeholders aligned.
 */
const RUNNING_WORKFLOW_RUN_OWNED_WHERE = `
      "namespace_id" = ?
      AND "id" = ?
      AND "status" = 'running'
      AND "worker_id" = ?`;

/**
 * WHERE fragment matching a step attempt whose parent workflow run is running
 * and held by the given worker. Consumes 6 positional
 * placeholders: step attempt's namespace, workflow_run_id, id, then the
 * parent workflow run's namespace, id, worker_id. Pair with
 * {@link BackendSqlite#runningStepAttemptOwnedParams} to keep the
 * placeholders aligned.
 */
const STEP_ATTEMPT_OWNED_WHERE = `
      "namespace_id" = ?
      AND "workflow_run_id" = ?
      AND "id" = ?
      AND EXISTS (
        SELECT 1
        FROM "workflow_runs" wr
        WHERE wr."namespace_id" = ?
        AND wr."id" = ?
        AND wr."status" = 'running'
        AND wr."worker_id" = ?
      )`;

/**
 * Manages a connection to a SQLite database for workflow operations.
 */
export class BackendSqlite implements Backend {
  private db: Database;
  private namespaceId: string;
  private ownsDatabase: boolean;

  private constructor(db: Database, namespaceId: string, ownsDatabase = true) {
    this.db = db;
    this.namespaceId = namespaceId;
    this.ownsDatabase = ownsDatabase;
  }

  /**
   * Create and initialize a new BackendSqlite instance. This will
   * automatically run migrations on startup unless `runMigrations` is set to
   * false.
   * @param path - Database path
   * @param options - Backend options
   * @returns A connected backend instance
   * @throws {Error} Error connecting to the SQLite database
   */
  static connect(path: string, options?: BackendSqliteOptions): BackendSqlite {
    const { namespaceId, runMigrations } = {
      namespaceId: DEFAULT_NAMESPACE_ID,
      runMigrations: true,
      ...options,
    };

    let db: Database | undefined;
    try {
      db = newDatabase(path);

      if (runMigrations) {
        migrate(db);
      }

      return new BackendSqlite(db, namespaceId);
    } catch (error) {
      db?.close();
      throw wrapError(
        "SQLite backend failed to open database. Check the path is valid and writable.",
        error,
      );
    }
  }

  /**
   * Create a BackendSqlite instance on a database connection the application
   * already has open, so the application's own writes and run creation can
   * share a transaction (see {@link BackendSqlite#createWorkflowRunSync}).
   * This will automatically run migrations unless `runMigrations` is set to
   * false. It sets no PRAGMAs, and `stop()` leaves the connection open.
   * Transactions the application opens on this connection must be synchronous
   * (no `await` between BEGIN and COMMIT), because the backend runs its own
   * transactions on the same connection whenever the application yields.
   * @param db - Open database connection
   * @param options - Backend options
   * @returns A backend instance using the connection
   */
  static fromDatabase(
    db: Database,
    options?: BackendSqliteOptions,
  ): BackendSqlite {
    const { namespaceId, runMigrations } = {
      namespaceId: DEFAULT_NAMESPACE_ID,
      runMigrations: true,
      ...options,
    };

    if (runMigrations) {
      migrate(db);
    }

    return new BackendSqlite(db, namespaceId, false);
  }

  /* oxlint-disable typescript/require-await -- SQLite operations must finish synchronously, while this interface returns rejected promises on failure. */
  async upsertWorkflowSchedule(
    params: WorkflowScheduleDefinition,
  ): Promise<WorkflowSchedule> {
    const now = new Date();
    // safety: RETURNING selects the schedule columns defined by our migration.
    const row = this.db
      .prepare(
        `
      INSERT INTO workflow_schedules
        (namespace_id,id,workflow_name,input,cron,timezone,next_run_at,created_at,updated_at,
         concurrency_key,overlap)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(namespace_id,id) DO UPDATE SET
        workflow_name=excluded.workflow_name, input=excluded.input,
        cron=excluded.cron, timezone=excluded.timezone, updated_at=excluded.updated_at,
        concurrency_key=excluded.concurrency_key, overlap=excluded.overlap,
        next_run_at=CASE WHEN workflow_schedules.next_run_at IS NOT NULL
          AND (workflow_schedules.cron<>excluded.cron OR workflow_schedules.timezone<>excluded.timezone)
          THEN excluded.next_run_at ELSE workflow_schedules.next_run_at END
      RETURNING *
    `,
      )
      .get(
        this.namespaceId,
        params.id,
        params.workflowName,
        toJSON(params.input),
        params.cron,
        params.timezone,
        nextScheduleOccurrence(params, now).toISOString(),
        now.toISOString(),
        now.toISOString(),
        params.concurrencyKey ?? null,
        params.overlap ?? "allow",
      ) as WorkflowScheduleRow;
    return rowToWorkflowSchedule(row);
  }

  async getWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<WorkflowSchedule | null> {
    // safety: SELECT reads a persisted schedule row.
    const row = this.db
      .prepare("SELECT * FROM workflow_schedules WHERE namespace_id=? AND id=?")
      .get(this.namespaceId, params.workflowScheduleId) as
      WorkflowScheduleRow | undefined;
    return row ? rowToWorkflowSchedule(row) : null;
  }

  async listWorkflowSchedules(
    params: PaginationOptions,
  ): Promise<PaginatedResponse<WorkflowSchedule>> {
    const cursor = decodeListCursor(params);
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const values: unknown[] = [this.namespaceId];
    let where = "namespace_id=?";
    if (cursor) {
      where += ` AND (created_at,id) ${params.before ? "<" : ">"} (?,?)`;
      values.push(cursor.createdAt.toISOString(), cursor.id);
    }
    const order = params.before ? "DESC" : "ASC";
    // safety: SELECT reads schedule rows in cursor order.
    const rows = this.db
      .prepare(
        `SELECT * FROM workflow_schedules WHERE ${where}
      ORDER BY created_at ${order},id ${order} LIMIT ?`,
      )
      .all(...values, limit + 1) as WorkflowScheduleRow[];
    return buildPaginatedResponse(
      rows.map((row) => rowToWorkflowSchedule(row)),
      limit,
      !!params.after,
      !!params.before,
    );
  }

  async pauseWorkflowSchedule(
    params: WorkflowScheduleIdParams & { paused: boolean },
  ): Promise<WorkflowSchedule> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      // safety: SELECT reads the schedule while holding the writer lock.
      let row = this.db
        .prepare(
          "SELECT * FROM workflow_schedules WHERE namespace_id=? AND id=?",
        )
        .get(this.namespaceId, params.workflowScheduleId) as
        WorkflowScheduleRow | undefined;
      requireRow(row, "change schedule pause state");
      if ((row.next_run_at === null) !== params.paused) {
        const now = new Date();
        const next = params.paused ? null : nextScheduleOccurrence(row, now);
        // safety: the locked schedule exists and UPDATE returns its columns.
        row = this.db
          .prepare(
            `UPDATE workflow_schedules SET next_run_at=?,updated_at=?
          WHERE namespace_id=? AND id=? RETURNING *`,
          )
          .get(
            toISO(next),
            now.toISOString(),
            this.namespaceId,
            params.workflowScheduleId,
          ) as WorkflowScheduleRow;
      }
      this.db.exec("COMMIT");
      return rowToWorkflowSchedule(row);
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  async deleteWorkflowSchedule(
    params: WorkflowScheduleIdParams,
  ): Promise<void> {
    this.db
      .prepare("DELETE FROM workflow_schedules WHERE namespace_id=? AND id=?")
      .run(this.namespaceId, params.workflowScheduleId);
  }

  async enqueueScheduledRuns(
    params: EnqueueScheduledRunsParams,
  ): Promise<number> {
    const before = params.before.toISOString();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      // safety: SELECT reads due schedules while holding the writer lock.
      const rows = this.db
        .prepare(
          `SELECT id,workflow_name,input,cron,timezone,next_run_at,concurrency_key,overlap
        FROM workflow_schedules
        WHERE namespace_id=? AND next_run_at<=? ORDER BY next_run_at,id LIMIT ?`,
        )
        .all(this.namespaceId, before, params.limit) as Pick<
        WorkflowScheduleRow,
        | "id"
        | "workflow_name"
        | "input"
        | "cron"
        | "timezone"
        | "next_run_at"
        | "concurrency_key"
        | "overlap"
      >[];
      const update = this.db
        .prepare(`UPDATE workflow_schedules SET next_run_at=?,updated_at=?
        WHERE namespace_id=? AND id=?`);
      // the id is derived from the schedule and the tick's nominal time, so a
      // tick creates at most one run however often it is processed
      const insert = this.db.prepare(`INSERT INTO workflow_runs
        (namespace_id,id,workflow_name,status,config,input,attempts,
         workflow_schedule_id,available_at,concurrency_key,created_at,updated_at)
        VALUES (?,?,?,'pending','{}',?,0,?,?,?,?,?)
        ON CONFLICT (namespace_id,id) DO NOTHING`);
      const unfinished = this.db.prepare(`SELECT 1 FROM workflow_runs
        WHERE namespace_id=? AND workflow_schedule_id=?
          AND status IN ('pending','running','sleeping')
        LIMIT 1`);
      for (const row of rows) {
        update.run(
          nextScheduleOccurrence(row, params.before).toISOString(),
          before,
          this.namespaceId,
          row.id,
        );
        if (
          row.overlap === "skip" &&
          unfinished.get(this.namespaceId, row.id)
        ) {
          continue;
        }
        const currentTime = now();
        insert.run(
          this.namespaceId,
          `__schedule:${row.id}:${String(row.next_run_at)}`,
          row.workflow_name,
          row.input,
          row.id,
          before,
          row.concurrency_key,
          currentTime,
          currentTime,
        );
      }
      this.db.exec("COMMIT");
      return rows.length;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  /* oxlint-enable typescript/require-await */

  // oxlint-disable-next-line typescript/require-await
  async stop(): Promise<void> {
    if (this.ownsDatabase) this.db.close();
  }

  /**
   * Create a workflow run synchronously on this connection without opening a
   * transaction, so it commits or rolls back with the caller's open
   * transaction. Idempotency keys are not accepted because their lookup opens
   * a transaction; pass `id` instead.
   * @param params - Workflow run params
   * @returns The created workflow run, or the existing run with `params.id`
   * @throws {Error} When `idempotencyKey` is set or the insert fails
   */
  createWorkflowRunSync(params: CreateWorkflowRunParams): WorkflowRun {
    if (params.idempotencyKey !== null) {
      throw new Error(
        "createWorkflowRunSync does not accept an idempotencyKey; pass id instead",
      );
    }

    return this.insertWorkflowRun(params);
  }

  createWorkflowRun(params: CreateWorkflowRunParams): Promise<WorkflowRun> {
    const { workflowName, idempotencyKey } = params;

    if (idempotencyKey === null) {
      return Promise.resolve(this.createWorkflowRunSync(params));
    }

    try {
      this.db.exec("BEGIN IMMEDIATE");

      const existing = this.getWorkflowRunByIdempotencyKey(
        workflowName,
        idempotencyKey,
        new Date(Date.now() - DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS),
      );
      if (existing) {
        this.db.exec("COMMIT");
        return Promise.resolve(existing);
      }

      const workflowRun = this.insertWorkflowRun(params);
      this.db.exec("COMMIT");
      return Promise.resolve(workflowRun);
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // ignore
      }

      return Promise.reject(
        error instanceof Error ? error : new Error(String(error)),
      );
    }
  }

  // oxlint-disable-next-line typescript/require-await -- keep the transaction synchronous
  async rerunWorkflowRun(
    request: RerunWorkflowRunParams,
  ): Promise<WorkflowRun> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      // safety: these queries select rows from the corresponding tables.
      const source = this.db
        .prepare(
          `
        SELECT * FROM "workflow_runs" WHERE "namespace_id" = ? AND "id" = ?
      `,
        )
        .get(this.namespaceId, request.workflowRunId) as
        WorkflowRunRow | undefined;
      // safety: the query selects step names, nullable indices, and statuses.
      const steps =
        request.fromStep === null
          ? []
          : (this.db
              .prepare(
                `
        SELECT "step_name", "step_index", "status" FROM "step_attempts"
        WHERE "namespace_id" = ? AND "workflow_run_id" = ?
      `,
              )
              .all(this.namespaceId, request.workflowRunId) as Pick<
              StepAttemptRow,
              "step_name" | "step_index" | "status"
            >[]);
      const { params, stepIndex } = prepareWorkflowRerun(
        source ? rowToWorkflowRun(source) : null,
        request,
        steps.map((step) => ({
          stepName: step.step_name,
          stepIndex: step.step_index,
          // safety: backend transitions write domain status values.
          status: step.status as StepAttempt["status"],
        })),
      );
      const run = this.insertWorkflowRun({
        ...params,
        concurrencyKey: source?.concurrency_key ?? null,
      });
      if (stepIndex !== null) {
        this.db
          .prepare(
            `
          INSERT INTO "step_attempts" (
            "namespace_id", "id", "workflow_run_id", "step_name", "step_index", "kind", "status",
            "config", "context", "output", "child_workflow_run_namespace_id", "child_workflow_run_id",
            "started_at", "finished_at", "created_at", "updated_at"
          )
          SELECT "namespace_id",
            lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||
              substr(hex(randomblob(2)), 2) || '-' || substr('89ab', (random() & 3) + 1, 1) ||
              substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6))),
            ?, "step_name", "step_index", "kind", "status",
            "config", "context", "output", "child_workflow_run_namespace_id", "child_workflow_run_id",
            "started_at", "finished_at", "created_at", "updated_at"
          FROM "step_attempts"
          WHERE "namespace_id" = ? AND "workflow_run_id" = ?
            AND "status" IN ('completed', 'succeeded')
            AND "step_index" < ?
        `,
          )
          .run(run.id, this.namespaceId, request.workflowRunId, stepIndex);
      }
      this.db.exec("COMMIT");
      return run;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private insertWorkflowRun(params: CreateWorkflowRunParams): WorkflowRun {
    this.assertChildKeyDiffersFromParent(params);
    const id = params.id ?? generateUUID();
    const currentTime = now();
    const availableAt = params.availableAt
      ? toISO(params.availableAt)
      : currentTime;
    const parentStepAttemptNamespaceId =
      params.parentStepAttemptNamespaceId ?? null;
    const parentStepAttemptId = params.parentStepAttemptId ?? null;

    const stmt = this.db.prepare(`
      INSERT INTO "workflow_runs" (
        "namespace_id",
        "id",
        "workflow_name",
        "version",
        "status",
        "idempotency_key",
        "config",
        "context",
        "input",
        "attempts",
        "parent_step_attempt_namespace_id",
        "parent_step_attempt_id",
        "available_at",
        "deadline_at",
        "concurrency_key",
        "created_at",
        "updated_at"
      )
      VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT ("namespace_id", "id") DO NOTHING
    `);

    stmt.run(
      this.namespaceId,
      id,
      params.workflowName,
      params.version,
      params.idempotencyKey,
      toJSON(params.config),
      toJSON(params.context),
      toJSON(params.input),
      parentStepAttemptNamespaceId,
      parentStepAttemptId,
      availableAt,
      toISO(params.deadlineAt),
      params.concurrencyKey ?? null,
      currentTime,
      currentTime,
    );

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = this.db
      .prepare(
        `
      SELECT *
      FROM "workflow_runs"
      WHERE "namespace_id" = ? AND "id" = ?
      LIMIT 1
    `,
      )
      .get(this.namespaceId, id) as WorkflowRunRow | null | undefined;
    requireRow(row, "create workflow run");

    return rowToWorkflowRun(row);
  }

  /**
   * Refuse a child run whose concurrency key is its parent's. The parent holds
   * its key while it waits, so the child could never be claimed.
   * @param params - Workflow run params
   * @throws {Error} When the child's key equals the parent run's key
   */
  private assertChildKeyDiffersFromParent(
    params: CreateWorkflowRunParams,
  ): void {
    if (
      (params.concurrencyKey ?? null) === null ||
      params.parentStepAttemptId === null
    ) {
      return;
    }

    // safety: the query selects the parent run's nullable concurrency_key column.
    const parent = this.db
      .prepare(
        `
      SELECT wr."concurrency_key"
      FROM "step_attempts" sa
      JOIN "workflow_runs" wr
        ON wr."namespace_id" = sa."namespace_id"
        AND wr."id" = sa."workflow_run_id"
      WHERE sa."namespace_id" = ? AND sa."id" = ?
    `,
      )
      .get(
        params.parentStepAttemptNamespaceId ?? this.namespaceId,
        params.parentStepAttemptId,
      ) as { concurrency_key: string | null } | undefined;
    if (parent?.concurrency_key === params.concurrencyKey) {
      throw new Error(
        `A child workflow run cannot use its parent's concurrency key "${String(params.concurrencyKey)}": the parent holds the key while it waits, so the child would never run`,
      );
    }
  }

  private getWorkflowRunByIdempotencyKey(
    workflowName: string,
    idempotencyKey: string,
    createdAt: Date,
  ): WorkflowRun | null {
    const stmt = this.db.prepare(`
      SELECT *
      FROM "workflow_runs"
      WHERE "namespace_id" = ?
        AND "workflow_name" = ?
        AND "idempotency_key" = ?
        AND "created_at" >= ?
      ORDER BY "created_at" ASC, "id" ASC
      LIMIT 1
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      this.namespaceId,
      workflowName,
      idempotencyKey,
      toISO(createdAt),
    ) as WorkflowRunRow | null | undefined;
    return row ? rowToWorkflowRun(row) : null;
  }

  getWorkflowRun(params: GetWorkflowRunParams): Promise<WorkflowRun | null> {
    const stmt = this.db.prepare(`
      SELECT *
      FROM "workflow_runs"
      WHERE "namespace_id" = ? AND "id" = ?
      LIMIT 1
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(this.namespaceId, params.workflowRunId) as
      WorkflowRunRow | null | undefined;

    return Promise.resolve(row ? rowToWorkflowRun(row) : null);
  }

  sendSignal(params: SendSignalParams): Promise<SendSignalResult> {
    const currentTime = now();

    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (params.idempotencyKey !== null) {
        const existingStmt = this.db.prepare(`
          SELECT DISTINCT "workflow_run_id"
          FROM "workflow_signals"
          WHERE "namespace_id" = ? AND "signal" = ? AND "sender_idempotency_key" = ?
        `);
        // safety: the query selects these non-null text identifiers from the signal and step tables.
        const existing = existingStmt.all(
          this.namespaceId,
          params.signal,
          params.idempotencyKey,
        ) as { workflow_run_id: string }[];
        if (existing.length > 0) {
          this.db.exec("COMMIT");
          return Promise.resolve({
            workflowRunIds: existing.map((r) => r.workflow_run_id),
          });
        }
      }
      const waitersStmt = this.db.prepare(`
        SELECT sa."id", sa."workflow_run_id"
        FROM "workflow_runs" wr
        JOIN "step_attempts" sa
          ON sa."namespace_id" = wr."namespace_id"
          AND sa."workflow_run_id" = wr."id"
        WHERE sa."namespace_id" = ?
          AND sa."kind" = 'signal-wait'
          AND sa."status" = 'running'
          AND json_extract(sa."context", '$.signal') = ?
          AND wr."status" IN ('pending', 'running', 'sleeping')
      `);
      // safety: the query selects these non-null text identifiers from the signal and step tables.
      const waiters = waitersStmt.all(this.namespaceId, params.signal) as {
        id: string;
        workflow_run_id: string;
      }[];

      if (waiters.length === 0) {
        this.db.exec("COMMIT");
        return Promise.resolve({ workflowRunIds: [] });
      }

      const insertStmt = this.db.prepare(`
        INSERT OR IGNORE INTO "workflow_signals" (
          "namespace_id", "id", "signal", "data",
          "sender_idempotency_key", "workflow_run_id",
          "step_attempt_id", "created_at"
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);
      const deliveredRunIds = new Set<string>();
      for (const w of waiters) {
        const result = insertStmt.run(
          this.namespaceId,
          generateUUID(),
          params.signal,
          toJSON(params.data),
          params.idempotencyKey,
          w.workflow_run_id,
          w.id,
          currentTime,
        );
        if (result.changes > 0) {
          deliveredRunIds.add(w.workflow_run_id);
        }
      }

      if (deliveredRunIds.size === 0) {
        this.db.exec("COMMIT");
        return Promise.resolve({ workflowRunIds: [] });
      }

      // wake each waiting workflow run
      const runIds = [...deliveredRunIds];
      const wakeStmt = this.db.prepare(`
        UPDATE "workflow_runs"
        SET
          "available_at" = CASE
            WHEN "available_at" IS NULL OR "available_at" > ? THEN ?
            ELSE "available_at"
          END,
          "updated_at" = ?
        WHERE "namespace_id" = ?
          AND "id" = ?
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "worker_id" IS NULL
      `);
      for (const runId of runIds) {
        wakeStmt.run(
          currentTime,
          currentTime,
          currentTime,
          this.namespaceId,
          runId,
        );
      }

      this.db.exec("COMMIT");
      return Promise.resolve({ workflowRunIds: runIds });
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  getSignalDelivery(
    params: GetSignalDeliveryParams,
  ): Promise<JsonValue | undefined> {
    const stmt = this.db.prepare(`
      SELECT "data"
      FROM "workflow_signals"
      WHERE "namespace_id" = ? AND "step_attempt_id" = ?
      LIMIT 1
    `);
    // safety: the query selects the nullable JSON text column from the signal delivery.
    const row = stmt.get(this.namespaceId, params.stepAttemptId) as
      { data: string | null } | null | undefined;

    // oxlint-disable-next-line unicorn/no-useless-undefined
    if (!row) return Promise.resolve<JsonValue | undefined>(undefined);
    return Promise.resolve(fromJSON(row.data));
  }

  /**
   * Send a message synchronously on this connection without opening a
   * transaction, so it commits or rolls back with the caller's open
   * transaction. A run created earlier in the same transaction can receive
   * it.
   * @param params - Message params
   * @returns The stored message's id, or the original message's id when the
   * idempotency key was already used for this run
   * @throws {Error} When the run does not exist or has already finished
   */
  sendMessageSync(params: SendMessageParams): SendMessageResult {
    if (params.idempotencyKey !== null) {
      // safety: the query selects the non-null text id of a stored message.
      const existing = this.db
        .prepare(
          `
        SELECT "id" FROM "workflow_messages"
        WHERE "namespace_id" = ? AND "workflow_run_id" = ? AND "idempotency_key" = ?
      `,
        )
        .get(this.namespaceId, params.workflowRunId, params.idempotencyKey) as
        { id: string } | undefined;
      if (existing) return { messageId: existing.id };
    }

    // safety: the query selects the run's non-null status column.
    const run = this.db
      .prepare(
        `SELECT "status" FROM "workflow_runs" WHERE "namespace_id" = ? AND "id" = ?`,
      )
      .get(this.namespaceId, params.workflowRunId) as
      { status: WorkflowRun["status"] } | undefined;
    if (!run) {
      throw new Error(`Workflow run ${params.workflowRunId} does not exist`);
    }
    if (isTerminalStatus(run.status)) {
      throw new Error(
        `Cannot send a message to workflow run ${params.workflowRunId} with status ${run.status}`,
      );
    }

    const id = generateUUID();
    const currentTime = now();
    this.db
      .prepare(
        `
      INSERT INTO "workflow_messages" (
        "namespace_id", "id", "workflow_run_id", "name", "data",
        "idempotency_key", "created_at"
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
      )
      .run(
        this.namespaceId,
        id,
        params.workflowRunId,
        params.name,
        toJSON(params.data),
        params.idempotencyKey,
        currentTime,
      );

    // wake the run only if it is parked waiting for this name
    this.db
      .prepare(
        `
      UPDATE "workflow_runs"
      SET
        "available_at" = CASE
          WHEN "available_at" IS NULL OR "available_at" > ? THEN ?
          ELSE "available_at"
        END,
        "updated_at" = ?
      WHERE "namespace_id" = ?
        AND "id" = ?
        AND "status" IN ('pending', 'running', 'sleeping')
        AND "worker_id" IS NULL
        AND EXISTS (
          SELECT 1
          FROM "step_attempts" sa
          WHERE sa."namespace_id" = "workflow_runs"."namespace_id"
          AND sa."workflow_run_id" = "workflow_runs"."id"
          AND sa."kind" = 'message-wait'
          AND sa."status" = 'running'
          AND json_extract(sa."context", '$.name') = ?
        )
    `,
      )
      .run(
        currentTime,
        currentTime,
        currentTime,
        this.namespaceId,
        params.workflowRunId,
        params.name,
      );

    return { messageId: id };
  }

  // oxlint-disable-next-line typescript/require-await -- keep the transaction synchronous
  async sendMessage(params: SendMessageParams): Promise<SendMessageResult> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = this.sendMessageSync(params);
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  // oxlint-disable-next-line typescript/require-await -- keep the transaction synchronous
  async consumeMessage(
    params: ConsumeMessageParams,
  ): Promise<WorkflowMessage | null> {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      // safety: these queries select workflow_messages columns defined by our SQLite migrations.
      const claimed = this.db
        .prepare(
          `
        SELECT * FROM "workflow_messages"
        WHERE "namespace_id" = ? AND "consumed_by_step_attempt_id" = ?
      `,
        )
        .get(this.namespaceId, params.stepAttemptId) as
        WorkflowMessageRow | undefined;
      // safety: these queries select workflow_messages columns defined by our SQLite migrations.
      const row =
        claimed ??
        (this.db
          .prepare(
            `
        UPDATE "workflow_messages"
        SET "consumed_by_step_attempt_id" = ?, "consumed_at" = ?
        WHERE "rowid" = (
          SELECT "rowid" FROM "workflow_messages"
          WHERE "namespace_id" = ?
            AND "workflow_run_id" = ?
            AND "name" = ?
            AND "consumed_by_step_attempt_id" IS NULL
          ORDER BY "created_at", "rowid"
          LIMIT 1
        )
        AND EXISTS (
          SELECT 1
          FROM "step_attempts"
          WHERE "namespace_id" = ?
          AND "id" = ?
          AND "workflow_run_id" = ?
          AND "kind" = 'message-wait'
          AND "status" = 'running'
        )
        RETURNING *
      `,
          )
          .get(
            params.stepAttemptId,
            now(),
            this.namespaceId,
            params.workflowRunId,
            params.name,
            this.namespaceId,
            params.stepAttemptId,
            params.workflowRunId,
          ) as WorkflowMessageRow | undefined);
      this.db.exec("COMMIT");
      return row ? rowToWorkflowMessage(row) : null;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  listMessages(
    params: ListMessagesParams,
  ): Promise<PaginatedResponse<WorkflowMessage>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const conditions = ['"namespace_id" = ?', '"workflow_run_id" = ?'];
    const values: unknown[] = [this.namespaceId, params.workflowRunId];
    if (cursor) {
      const op = before ? "<" : ">";
      conditions.push(`("created_at", "id") ${op} (?, ?)`);
      values.push(cursor.createdAt.toISOString(), cursor.id);
    }

    const order = before ? "DESC" : "ASC";
    const stmt = this.db.prepare(`
      SELECT * FROM "workflow_messages"
      WHERE ${conditions.join(" AND ")}
      ORDER BY "created_at" ${order}, "id" ${order}
      LIMIT ?
    `);
    // safety: this query selects workflow_messages columns defined by our SQLite migrations.
    const rows = stmt.all(...values, limit + 1) as WorkflowMessageRow[];
    return Promise.resolve(
      buildPaginatedResponse(
        rows.map((row) => rowToWorkflowMessage(row)),
        limit,
        !!after,
        !!before,
      ),
    );
  }

  async claimWorkflowRun(
    params: ClaimWorkflowRunParams,
  ): Promise<WorkflowRun | null> {
    const currentTime = now();
    const newAvailableAt = addMilliseconds(currentTime, params.leaseDurationMs);

    // SQLite doesn't have SKIP LOCKED, so we need to handle claims differently
    this.db.exec("BEGIN IMMEDIATE");

    try {
      // 1. mark any deadline-expired workflow runs as failed
      const expireStmt = this.db.prepare(`
        UPDATE "workflow_runs"
        SET
          "status" = 'failed',
          "error" = ?,
          "worker_id" = NULL,
          "available_at" = NULL,
          "finished_at" = ?,
          "updated_at" = ?
        WHERE "namespace_id" = ?
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "deadline_at" IS NOT NULL
          AND "deadline_at" <= ?
      `);

      expireStmt.run(
        toJSON({ message: "Workflow run deadline exceeded" }),
        currentTime,
        currentTime,
        this.namespaceId,
        currentTime,
      );

      // 2. find an available workflow run to claim
      const findStmt = this.db.prepare(`
        SELECT "id"
        FROM "workflow_runs"
        WHERE "namespace_id" = ?
          AND "status" IN ('pending', 'running', 'sleeping')
          AND "available_at" <= ?
          AND ("deadline_at" IS NULL OR "deadline_at" > ?)
          AND ("concurrency_key" IS NULL OR NOT EXISTS (
            SELECT 1
            FROM "workflow_runs" o
            WHERE o."namespace_id" = "workflow_runs"."namespace_id"
            AND o."concurrency_key" = "workflow_runs"."concurrency_key"
            AND o."attempts" > 0
            AND o."status" IN ('pending', 'running', 'sleeping')
            AND o."id" <> "workflow_runs"."id"
          ))
        ORDER BY
          CASE WHEN "status" = 'pending' THEN 0 ELSE 1 END,
          "available_at",
          "created_at",
          "rowid"
        LIMIT 1
      `);

      // safety: the query selects the workflow_runs non-null text id column, or returns no row.
      const candidate = findStmt.get(
        this.namespaceId,
        currentTime,
        currentTime,
      ) as { id: string } | null | undefined;

      if (!candidate) {
        this.db.exec("COMMIT");
        return null;
      }

      // 3. claim the workflow run
      const claimStmt = this.db.prepare(`
        UPDATE "workflow_runs"
        SET
          "status" = 'running',
          "attempts" = "attempts" + 1,
          "worker_id" = ?,
          "available_at" = ?,
          "started_at" = COALESCE("started_at", ?),
          "updated_at" = ?
        WHERE "id" = ?
          AND "namespace_id" = ?
      `);

      claimStmt.run(
        params.workerId,
        newAvailableAt,
        currentTime,
        currentTime,
        candidate.id,
        this.namespaceId,
      );

      this.db.exec("COMMIT");

      return await this.getWorkflowRun({ workflowRunId: candidate.id });
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  async extendWorkflowRunLease(
    params: ExtendWorkflowRunLeaseParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();
    const newAvailableAt = addMilliseconds(currentTime, params.leaseDurationMs);

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "available_at" = ?,
        "updated_at" = ?
      WHERE ${RUNNING_WORKFLOW_RUN_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      newAvailableAt,
      currentTime,
      ...this.runningWorkflowRunOwnedParams(params),
    ) as WorkflowRunRow | null | undefined;
    requireRow(row, "extend lease for workflow run");

    return await Promise.resolve(rowToWorkflowRun(row));
  }

  async sleepWorkflowRun(params: SleepWorkflowRunParams): Promise<WorkflowRun> {
    const currentTime = now();
    const resumeAt = toISO(params.availableAt);

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "status" = 'running',
        "available_at" = CASE
          WHEN (
            EXISTS (
              SELECT 1
              FROM "step_attempts" sa
              JOIN "workflow_runs" child
                ON child."namespace_id" = sa."child_workflow_run_namespace_id"
                AND child."id" = sa."child_workflow_run_id"
              WHERE sa."namespace_id" = "workflow_runs"."namespace_id"
              AND sa."workflow_run_id" = "workflow_runs"."id"
              AND sa."kind" = 'workflow'
              AND sa."status" = 'running'
              AND child."status" IN ('completed', 'succeeded', 'failed', 'canceled')
            )
            OR EXISTS (
              SELECT 1
              FROM "step_attempts" sa
              JOIN "workflow_signals" ws
                ON ws."namespace_id" = sa."namespace_id"
                AND ws."step_attempt_id" = sa."id"
              WHERE sa."namespace_id" = "workflow_runs"."namespace_id"
              AND sa."workflow_run_id" = "workflow_runs"."id"
              AND sa."kind" = 'signal-wait'
              AND sa."status" = 'running'
            )
            OR EXISTS (
              SELECT 1
              FROM "step_attempts" sa
              JOIN "workflow_messages" wm
                ON wm."namespace_id" = sa."namespace_id"
                AND wm."workflow_run_id" = sa."workflow_run_id"
                AND wm."name" = json_extract(sa."context", '$.name')
                AND wm."consumed_by_step_attempt_id" IS NULL
              WHERE sa."namespace_id" = "workflow_runs"."namespace_id"
              AND sa."workflow_run_id" = "workflow_runs"."id"
              AND sa."kind" = 'message-wait'
              AND sa."status" = 'running'
            )
          ) AND ? > ? THEN ?
          ELSE ?
        END,
        "worker_id" = NULL,
        "updated_at" = ?
      WHERE "namespace_id" = ?
      AND "id" = ?
      AND "status" NOT IN ('succeeded', 'completed', 'failed', 'canceled')
      AND "worker_id" = ?
      RETURNING *
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      resumeAt,
      currentTime,
      currentTime,
      resumeAt,
      currentTime,
      this.namespaceId,
      params.workflowRunId,
      params.workerId,
    ) as WorkflowRunRow | null | undefined;
    requireRow(row, "sleep workflow run");

    return await Promise.resolve(rowToWorkflowRun(row));
  }

  async completeWorkflowRun(
    params: CompleteWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "status" = 'completed',
        "output" = ?,
        "error" = NULL,
        "worker_id" = ?,
        "available_at" = NULL,
        "finished_at" = ?,
        "updated_at" = ?
      WHERE ${RUNNING_WORKFLOW_RUN_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      toJSON(params.output),
      params.workerId,
      currentTime,
      currentTime,
      ...this.runningWorkflowRunOwnedParams(params),
    ) as WorkflowRunRow | null | undefined;
    requireRow(row, "mark workflow run completed");

    const updated = rowToWorkflowRun(row);
    this.wakeParentWorkflowRun(updated);
    return await Promise.resolve(updated);
  }

  async failWorkflowRun(params: FailWorkflowRunParams): Promise<WorkflowRun> {
    const { workflowRunId, error } = params;
    const currentTime = new Date();
    const currentTimeIso = currentTime.toISOString();
    let attempts = params.attempts;
    let deadlineAt = params.deadlineAt;

    // Backward-compatible fallback for external callers that don't pass state.
    if (attempts === undefined || deadlineAt === undefined) {
      const workflowRun = await this.getWorkflowRun({ workflowRunId });
      if (!workflowRun) throw new Error("Workflow run not found");
      attempts = workflowRun.attempts;
      deadlineAt = workflowRun.deadlineAt;
    }

    const failureUpdate = computeFailedWorkflowRunUpdate(
      params.retryPolicy,
      attempts,
      deadlineAt,
      error,
      currentTime,
    );

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "status" = ?,
        "available_at" = ?,
        "finished_at" = ?,
        "error" = ?,
        "worker_id" = NULL,
        "started_at" = NULL,
        "updated_at" = ?
      WHERE ${RUNNING_WORKFLOW_RUN_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      failureUpdate.status,
      failureUpdate.availableAt?.toISOString() ?? null,
      failureUpdate.finishedAt?.toISOString() ?? null,
      toJSON(failureUpdate.error),
      currentTimeIso,
      ...this.runningWorkflowRunOwnedParams(params),
    ) as WorkflowRunRow | null | undefined;
    requireRow(row, "mark workflow run failed");
    const updated = rowToWorkflowRun(row);
    if (updated.status === "failed") {
      this.wakeParentWorkflowRun(updated);
    }
    return updated;
  }

  rescheduleWorkflowRunAfterFailedStepAttempt(
    params: RescheduleWorkflowRunAfterFailedStepAttemptParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "status" = 'pending',
        "available_at" = ?,
        "finished_at" = NULL,
        "error" = ?,
        "worker_id" = NULL,
        "started_at" = NULL,
        "updated_at" = ?
      WHERE ${RUNNING_WORKFLOW_RUN_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const row = stmt.get(
      toISO(params.availableAt),
      toJSON(params.error),
      currentTime,
      ...this.runningWorkflowRunOwnedParams(params),
    ) as WorkflowRunRow | null | undefined;
    if (!row) {
      return Promise.reject(
        new Error(
          "Failed to reschedule workflow run after failed step attempt",
        ),
      );
    }

    return Promise.resolve(rowToWorkflowRun(row));
  }

  async cancelWorkflowRun(
    params: CancelWorkflowRunParams,
  ): Promise<WorkflowRun> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "status" = 'canceled',
        "worker_id" = NULL,
        "available_at" = NULL,
        "finished_at" = ?,
        "updated_at" = ?
      WHERE "namespace_id" = ?
      AND "id" = ?
      AND "status" IN ('pending', 'running', 'sleeping')
    `);

    const result = stmt.run(
      currentTime,
      currentTime,
      this.namespaceId,
      params.workflowRunId,
    );

    if (result.changes === 0) {
      // workflow may already be in a terminal state
      const existing = await this.getWorkflowRun({
        workflowRunId: params.workflowRunId,
      });
      return resolveCancelWorkflowRunConflict(params.workflowRunId, existing);
    }

    const updated = await this.getWorkflowRun({
      workflowRunId: params.workflowRunId,
    });
    requireRow(updated, "cancel workflow run");

    this.wakeParentWorkflowRun(updated);

    return updated;
  }

  // oxlint-disable-next-line typescript/require-await -- keep the transaction synchronous
  async cancelWorkflowRunsByConcurrencyKey(
    params: CancelWorkflowRunsByConcurrencyKeyParams,
  ): Promise<CancelWorkflowRunsResult> {
    const currentTime = now();

    this.db.exec("BEGIN IMMEDIATE");
    try {
      // safety: RETURNING selects the non-null text id of each canceled run.
      const canceled = this.db
        .prepare(
          `
        UPDATE "workflow_runs"
        SET
          "status" = 'canceled',
          "worker_id" = NULL,
          "available_at" = NULL,
          "finished_at" = ?,
          "updated_at" = ?
        WHERE "namespace_id" = ?
        AND "concurrency_key" = ?
        AND "status" IN ('pending', 'running', 'sleeping')
        AND "id" IS NOT ?
        RETURNING "id"
      `,
        )
        .all(
          currentTime,
          currentTime,
          this.namespaceId,
          params.concurrencyKey,
          params.exceptWorkflowRunId,
        ) as { id: string }[];
      const workflowRunIds = canceled.map((row) => row.id);
      for (const id of workflowRunIds) {
        this.wakeParentWorkflowRun({ namespaceId: this.namespaceId, id });
      }
      this.db.exec("COMMIT");
      return { workflowRunIds };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  /**
   * Return positional placeholders for {@link RUNNING_WORKFLOW_RUN_OWNED_WHERE}
   * in the order the fragment expects: namespace, run id, worker id.
   * @param params - Workflow run identity params
   * @returns Tuple of placeholder values
   */
  private runningWorkflowRunOwnedParams(
    params: Readonly<{ workflowRunId: string; workerId: string }>,
  ): [string, string, string] {
    return [this.namespaceId, params.workflowRunId, params.workerId];
  }

  /**
   * Return positional placeholders for {@link STEP_ATTEMPT_OWNED_WHERE}
   * in the order the fragment expects.
   * @param params - Step attempt identity params
   * @returns Tuple of placeholder values
   */
  private runningStepAttemptOwnedParams(
    params: Readonly<{
      workflowRunId: string;
      stepAttemptId: string;
      workerId: string;
    }>,
  ): [string, string, string, string, string, string] {
    return [
      this.namespaceId,
      params.workflowRunId,
      params.stepAttemptId,
      this.namespaceId,
      params.workflowRunId,
      params.workerId,
    ];
  }

  /**
   * Wake every parked run with a running workflow step linked to the finished
   * run. That is its creating parent, and also any parent that chose the run
   * by id after it was created.
   * @param childWorkflowRun - Finished workflow run
   */
  private wakeParentWorkflowRun(
    childWorkflowRun: Readonly<Pick<WorkflowRun, "namespaceId" | "id">>,
  ): void {
    const currentTime = now();
    const stmt = this.db.prepare(`
      UPDATE "workflow_runs"
      SET
        "available_at" = CASE
          WHEN "available_at" IS NULL OR "available_at" > ? THEN ?
          ELSE "available_at"
        END,
        "updated_at" = ?
      WHERE "namespace_id" = ?
      AND "id" IN (
        SELECT "workflow_run_id"
        FROM "step_attempts"
        WHERE "namespace_id" = ?
        AND "kind" = 'workflow'
        AND "status" = 'running'
        AND "child_workflow_run_namespace_id" = ?
        AND "child_workflow_run_id" = ?
      )
      AND (
        "status" = 'sleeping'
        OR ("status" = 'running' AND "worker_id" IS NULL)
      )
    `);

    stmt.run(
      currentTime,
      currentTime,
      currentTime,
      this.namespaceId,
      this.namespaceId,
      childWorkflowRun.namespaceId,
      childWorkflowRun.id,
    );
  }

  // oxlint-disable-next-line typescript/require-await
  async countWorkflowRuns(): Promise<WorkflowRunCounts> {
    const stmt = this.db.prepare(`
      SELECT "status", COUNT(*) AS "count"
      FROM "workflow_runs"
      WHERE "namespace_id" = ?
      GROUP BY "status"
    `);

    // safety: the grouped query returns the stored status and COUNT(*) as a numeric count.
    const rows = stmt.all(this.namespaceId) as {
      status: string;
      count: number;
    }[];
    return toWorkflowRunCounts(rows);
  }

  listWorkflowRuns(
    params: ListWorkflowRunsParams,
  ): Promise<PaginatedResponse<WorkflowRun>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const conditions = ['"namespace_id" = ?'];
    const values: unknown[] = [this.namespaceId];

    if (params.status) {
      conditions.push(`"status" = ?`);
      values.push(params.status);
    }
    if (params.workflowName) {
      conditions.push(`"workflow_name" = ?`);
      values.push(params.workflowName);
    }
    if (params.concurrencyKey !== undefined) {
      conditions.push(`"concurrency_key" = ?`);
      values.push(params.concurrencyKey);
    }

    if (cursor) {
      const op = before ? ">" : "<";
      conditions.push(`("created_at", "id") ${op} (?, ?)`);
      values.push(cursor.createdAt.toISOString(), cursor.id);
    }

    const order = before ? "ASC" : "DESC";
    const stmt = this.db.prepare(`
      SELECT * FROM "workflow_runs"
      WHERE ${conditions.join(" AND ")}
      ORDER BY "created_at" ${order}, "id" ${order}
      LIMIT ?
    `);
    // safety: this query selects workflow_runs columns defined by our SQLite migrations.
    const rows = stmt.all(...values, limit + 1) as WorkflowRunRow[];
    return Promise.resolve(
      buildPaginatedResponse(
        rows.map((row) => rowToWorkflowRun(row)),
        limit,
        !!after,
        !!before,
      ),
    );
  }

  listStepAttempts(
    params: ListStepAttemptsParams,
  ): Promise<PaginatedResponse<StepAttempt>> {
    const limit = params.limit ?? DEFAULT_PAGINATION_PAGE_SIZE;
    const { after, before } = params;
    const cursor = decodeListCursor(params);

    const conditions = ['"namespace_id" = ?', '"workflow_run_id" = ?'];
    const values: unknown[] = [this.namespaceId, params.workflowRunId];
    if (cursor) {
      const op = before ? "<" : ">";
      conditions.push(
        `("created_at", COALESCE("step_index", -1), "id") ${op} (?, ?, ?)`,
      );
      values.push(
        cursor.createdAt.toISOString(),
        cursor.stepIndex ?? -1,
        cursor.id,
      );
    }

    const order = before ? "DESC" : "ASC";
    const stmt = this.db.prepare(`
      SELECT * FROM "step_attempts"
      WHERE ${conditions.join(" AND ")}
      ORDER BY "created_at" ${order}, "step_index" ${order}, "id" ${order}
      LIMIT ?
    `);
    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const rows = stmt.all(...values, limit + 1) as StepAttemptRow[];
    return Promise.resolve(
      buildPaginatedResponse(
        rows.map((row) => rowToStepAttempt(row)),
        limit,
        !!after,
        !!before,
      ),
    );
  }

  async createStepAttempt(
    params: CreateStepAttemptParams,
  ): Promise<StepAttempt> {
    const id = generateUUID();
    const currentTime = now();

    const stmt = this.db.prepare(`
      INSERT INTO "step_attempts" (
        "namespace_id",
        "id",
        "workflow_run_id",
        "step_name",
        "step_index",
        "kind",
        "status",
        "config",
        "context",
        "started_at",
        "created_at",
        "updated_at"
      )
      SELECT ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?
      FROM "workflow_runs"
      WHERE ${RUNNING_WORKFLOW_RUN_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const row = stmt.get(
      this.namespaceId,
      id,
      params.workflowRunId,
      params.stepName,
      params.stepIndex ?? null,
      params.kind,
      toJSON(params.config),
      toJSON(params.context),
      currentTime,
      currentTime,
      currentTime,
      ...this.runningWorkflowRunOwnedParams(params),
    ) as StepAttemptRow | null | undefined;
    requireRow(row, "create step attempt");

    return await Promise.resolve(rowToStepAttempt(row));
  }

  async setStepAttemptChildWorkflowRun(
    params: SetStepAttemptChildWorkflowRunParams,
  ): Promise<StepAttempt> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "step_attempts"
      SET
        "child_workflow_run_namespace_id" = ?,
        "child_workflow_run_id" = ?,
        "updated_at" = ?
      WHERE "status" = 'running'
      AND ${STEP_ATTEMPT_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const row = stmt.get(
      params.childWorkflowRunNamespaceId,
      params.childWorkflowRunId,
      currentTime,
      ...this.runningStepAttemptOwnedParams(params),
    ) as StepAttemptRow | null | undefined;
    requireRow(row, "set step attempt child workflow run");

    return await Promise.resolve(rowToStepAttempt(row));
  }

  getStepAttempt(params: GetStepAttemptParams): Promise<StepAttempt | null> {
    const stmt = this.db.prepare(`
      SELECT *
      FROM "step_attempts"
      WHERE "namespace_id" = ? AND "id" = ?
      LIMIT 1
    `);

    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const row = stmt.get(this.namespaceId, params.stepAttemptId) as
      StepAttemptRow | null | undefined;

    return Promise.resolve(row ? rowToStepAttempt(row) : null);
  }

  async completeStepAttempt(
    params: CompleteStepAttemptParams,
  ): Promise<StepAttempt> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "step_attempts"
      SET
        "status" = 'completed',
        "output" = CASE
          WHEN "status" = 'running' THEN ?
          ELSE "output"
        END,
        "error" = NULL,
        "finished_at" = COALESCE("finished_at", ?),
        "updated_at" = CASE
          WHEN "status" = 'running' THEN ?
          ELSE "updated_at"
        END
      WHERE "status" IN ('running', 'completed')
      AND ${STEP_ATTEMPT_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const row = stmt.get(
      toJSON(params.output),
      currentTime,
      currentTime,
      ...this.runningStepAttemptOwnedParams(params),
    ) as StepAttemptRow | null | undefined;
    requireRow(row, "mark step attempt completed");

    return await Promise.resolve(rowToStepAttempt(row));
  }

  async failStepAttempt(params: FailStepAttemptParams): Promise<StepAttempt> {
    const currentTime = now();

    const stmt = this.db.prepare(`
      UPDATE "step_attempts"
      SET
        "status" = 'failed',
        "output" = NULL,
        "error" = CASE
          WHEN "status" = 'running' THEN ?
          ELSE "error"
        END,
        "finished_at" = COALESCE("finished_at", ?),
        "updated_at" = CASE
          WHEN "status" = 'running' THEN ?
          ELSE "updated_at"
        END
      WHERE "status" IN ('running', 'failed')
      AND ${STEP_ATTEMPT_OWNED_WHERE}
      RETURNING *
    `);

    // safety: this query selects step_attempts columns defined by our SQLite migrations.
    const row = stmt.get(
      toJSON(params.error),
      currentTime,
      currentTime,
      ...this.runningStepAttemptOwnedParams(params),
    ) as StepAttemptRow | null | undefined;
    requireRow(row, "mark step attempt failed");

    return await Promise.resolve(rowToStepAttempt(row));
  }
}

// Row types for SQLite results
interface WorkflowRunRow extends Record<string, SQLOutputValue> {
  workflow_schedule_id: string | null;
  concurrency_key: string | null;
  namespace_id: string;
  id: string;
  workflow_name: string;
  version: string | null;
  status: string;
  idempotency_key: string | null;
  config: string;
  context: string | null;
  input: string | null;
  output: string | null;
  error: string | null;
  attempts: number;
  parent_step_attempt_namespace_id: string | null;
  parent_step_attempt_id: string | null;
  worker_id: string | null;
  available_at: string | null;
  deadline_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
}

interface StepAttemptRow extends Record<string, SQLOutputValue> {
  namespace_id: string;
  id: string;
  workflow_run_id: string;
  step_name: string;
  step_index: number | null;
  kind: string;
  status: string;
  config: string;
  context: string | null;
  output: string | null;
  error: string | null;
  child_workflow_run_namespace_id: string | null;
  child_workflow_run_id: string | null;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  updated_at: string;
}

// Conversion functions
interface ParsedRowFields {
  createdAt: Date;
  updatedAt: Date;
  config: Exclude<JsonValue, null>;
}

/**
 * Parse and validate the `created_at`, `updated_at`, and `config` fields
 * shared by workflow run and step attempt rows.
 * @param row - Row with timestamp and config columns
 * @returns Parsed createdAt/updatedAt dates and decoded config
 * @throws {Error} If any required field is missing
 */
function parseRequiredRowFields(
  row: Readonly<{ created_at: string; updated_at: string; config: string }>,
): ParsedRowFields {
  const createdAt = fromISO(row.created_at);
  const updatedAt = fromISO(row.updated_at);
  const config = fromJSON(row.config);

  if (!createdAt) throw new Error("createdAt is required");
  if (!updatedAt) throw new Error("updatedAt is required");
  if (config === null) throw new Error("config is required");

  return { createdAt, updatedAt, config };
}

/**
 * Convert a database row to a WorkflowRun.
 * @param row - Workflow run row
 * @returns Workflow run
 * @throws {Error} If required fields are missing
 */
function rowToWorkflowRun(row: WorkflowRunRow): WorkflowRun {
  const { createdAt, updatedAt, config } = parseRequiredRowFields(row);

  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowName: row.workflow_name,
    workflowScheduleId: row.workflow_schedule_id,
    version: row.version,
    // safety: the status column is written by backend transitions using the domain status values.
    status: row.status as WorkflowRun["status"],
    idempotencyKey: row.idempotency_key,
    config,
    context: fromJSON(row.context),
    input: fromJSON(row.input),
    output: fromJSON(row.output),
    // safety: workflow failures are persisted from the serialized error contract.
    error: fromJSON(row.error) as WorkflowRun["error"],
    attempts: row.attempts,
    parentStepAttemptNamespaceId: row.parent_step_attempt_namespace_id,
    parentStepAttemptId: row.parent_step_attempt_id,
    // omitted when there is none, so a keyless run has the same shape as on
    // backends without keys
    ...(row.concurrency_key !== null && {
      concurrencyKey: row.concurrency_key,
    }),
    workerId: row.worker_id,
    availableAt: fromISO(row.available_at),
    deadlineAt: fromISO(row.deadline_at),
    startedAt: fromISO(row.started_at),
    finishedAt: fromISO(row.finished_at),
    createdAt,
    updatedAt,
  };
}

/**
 * Convert a database row to a StepAttempt.
 * @param row - Step attempt row
 * @returns Step attempt
 * @throws {Error} If required fields are missing
 */
function rowToStepAttempt(row: StepAttemptRow): StepAttempt {
  const { createdAt, updatedAt, config } = parseRequiredRowFields(row);

  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowRunId: row.workflow_run_id,
    stepName: row.step_name,
    stepIndex: row.step_index,
    // safety: the kind column is written from CreateStepAttemptParams.kind.
    kind: row.kind as StepAttempt["kind"],
    // safety: the status column is written by backend transitions using the domain status values.
    status: row.status as StepAttempt["status"],
    config,
    // safety: step context is persisted from StepAttemptContext and decoded here with the same contract.
    context: fromJSON(row.context) as StepAttempt["context"],
    output: fromJSON(row.output),
    error: fromJSON(row.error),
    childWorkflowRunNamespaceId: row.child_workflow_run_namespace_id,
    childWorkflowRunId: row.child_workflow_run_id,
    startedAt: fromISO(row.started_at),
    finishedAt: fromISO(row.finished_at),
    createdAt,
    updatedAt,
  };
}

interface WorkflowMessageRow extends Record<string, SQLOutputValue> {
  namespace_id: string;
  id: string;
  workflow_run_id: string;
  name: string;
  data: string | null;
  idempotency_key: string | null;
  consumed_by_step_attempt_id: string | null;
  consumed_at: string | null;
  created_at: string;
}

/**
 * Convert a SQLite row to a message.
 * @param row - Persisted message
 * @returns Message with parsed JSON and timestamps
 */
function rowToWorkflowMessage(row: WorkflowMessageRow): WorkflowMessage {
  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowRunId: row.workflow_run_id,
    name: row.name,
    data: fromJSON(row.data),
    idempotencyKey: row.idempotency_key,
    consumedByStepAttemptId: row.consumed_by_step_attempt_id,
    consumedAt: fromISO(row.consumed_at),
    createdAt: new Date(row.created_at),
  };
}

interface WorkflowScheduleRow extends Record<string, SQLOutputValue> {
  namespace_id: string;
  id: string;
  workflow_name: string;
  input: string | null;
  cron: string;
  timezone: string;
  next_run_at: string | null;
  concurrency_key: string | null;
  overlap: string;
  created_at: string;
  updated_at: string;
}

/**
 * Convert a SQLite row to a schedule.
 * @param row - Persisted schedule
 * @returns Schedule with parsed JSON and timestamps
 */
function rowToWorkflowSchedule(row: WorkflowScheduleRow): WorkflowSchedule {
  return {
    namespaceId: row.namespace_id,
    id: row.id,
    workflowName: row.workflow_name,
    input: fromJSON(row.input),
    cron: row.cron,
    timezone: row.timezone,
    concurrencyKey: row.concurrency_key,
    // safety: the overlap column is written from WorkflowScheduleOverlap values.
    overlap: row.overlap as WorkflowScheduleOverlap,
    nextRunAt: fromISO(row.next_run_at),
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}
