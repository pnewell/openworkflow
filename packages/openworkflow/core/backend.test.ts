import {
  DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS,
  resolveIdempotencyCutoff,
  toWorkflowRunCounts,
} from "./backend.js";
import { describe, expect, test } from "vitest";

describe("toWorkflowRunCounts", () => {
  test("folds legacy succeeded and sleeping rows into normalized counts", () => {
    const counts = toWorkflowRunCounts([
      { status: "completed", count: 2 },
      { status: "succeeded", count: 3 },
      { status: "running", count: 1 },
      { status: "sleeping", count: 4 },
    ]);

    expect(counts).toEqual({
      pending: 0,
      running: 5,
      completed: 5,
      failed: 0,
      canceled: 0,
    });

    const reversedCounts = toWorkflowRunCounts([
      { status: "sleeping", count: 4 },
      { status: "running", count: 1 },
      { status: "succeeded", count: 3 },
      { status: "completed", count: 2 },
    ]);

    expect(reversedCounts).toEqual({
      pending: 0,
      running: 5,
      completed: 5,
      failed: 0,
      canceled: 0,
    });
  });

  test("ignores unknown statuses", () => {
    const counts = toWorkflowRunCounts([
      { status: "pending", count: 1 },
      { status: "unknown_status", count: 99 },
    ]);

    expect(counts).toEqual({
      pending: 1,
      running: 0,
      completed: 0,
      failed: 0,
      canceled: 0,
    });
  });
});

describe("resolveIdempotencyCutoff", () => {
  const now = Date.parse("2026-01-02T00:00:00.000Z");

  test("defaults to the 24 hour idempotency period", () => {
    expect(resolveIdempotencyCutoff(undefined, now)).toEqual(
      new Date(now - DEFAULT_RUN_IDEMPOTENCY_PERIOD_MS),
    );
  });

  test("subtracts the given period from now", () => {
    expect(resolveIdempotencyCutoff(60_000, now)).toEqual(
      new Date(now - 60_000),
    );
  });

  test("treats 0 as no limit", () => {
    expect(resolveIdempotencyCutoff(0, now)).toEqual(new Date(0));
  });
});
