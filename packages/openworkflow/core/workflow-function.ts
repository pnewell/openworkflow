import type { DurationString } from "./duration.js";
import type { JsonValue } from "./json.js";
import type { StandardSchemaV1 } from "./standard-schema.js";
import type { RetryPolicy, WorkflowSpec } from "./workflow-definition.js";
import type { WorkflowRun } from "./workflow-run.js";

/**
 * Timeout for a step wait. Accepts milliseconds (number), a duration string
 * (e.g. "5m", "1h"), or an absolute Date.
 */
export type StepWaitTimeout = number | string | Date;

/**
 * Config for an individual step defined with `step.run()`.
 */
export interface StepFunctionConfig {
  /**
   * The name of the step.
   */
  name: string;
  /**
   * Optional retry policy override for this step.
   */
  retryPolicy?: Partial<RetryPolicy>;
}

/**
 * The step definition (defined by the user) that executes user code. Can
 * return undefined (e.g., when using `return;`) which will be converted to
 * null.
 */
export type StepFunction<Output> = () =>
  Promise<Output | undefined> | Output | undefined;

/**
 * Options for an individual step defined with `step.runWorkflow()`.
 */
export interface StepRunWorkflowOptions {
  /**
   * Optional durable step name. Defaults to the target workflow name.
   */
  name?: string;
  /**
   * Maximum time to wait for the child workflow to complete.
   */
  timeout?: StepWaitTimeout;
  /**
   * Choose the child run's ID. If a run with this ID already exists, the
   * parent waits on that run and nothing new is created. SQLite only.
   */
  id?: string;
  /**
   * Give the child run a concurrency key, so it queues behind other runs with
   * the key. A parent with no key holds no key while it waits. A parent keeps
   * its own key while it waits, so the child's key must differ from the
   * parent's. SQLite only.
   */
  concurrencyKey?: string;
}

/**
 * Options for `step.startWorkflow()`.
 */
export interface StepStartWorkflowOptions {
  /**
   * Optional durable step name. Defaults to the target workflow name.
   */
  name?: string;
  /**
   * Choose the started run's ID. Defaults to an ID derived from this run's ID
   * and the step name, so the step can never start two runs. If a run with
   * the ID already exists, nothing is created and its ID is returned.
   */
  id?: string;
  /**
   * Give the started run a concurrency key, so it queues behind other runs
   * with the key. It may be this run's own key, since nothing waits.
   */
  concurrencyKey?: string;
  /**
   * Start the run later: an absolute Date or a duration string.
   */
  availableAt?: Date | DurationString;
  /**
   * Fail the started run if it has not finished by then.
   */
  deadlineAt?: Date;
}

/**
 * Options for `step.sendMessage()`.
 */
export interface StepSendMessageOptions {
  /** Workflow run the message is addressed to. */
  workflowRunId: string;
  /** Message name the receiving run waits on. */
  name: string;
  data?: JsonValue;
  /** Optional durable step name. Defaults to the message name. */
  stepName?: string;
}

/**
 * Options for `step.waitForMessage()`.
 */
export interface StepWaitForMessageOptions<Output> {
  /** Message name. Each wait takes the oldest unread message with it. */
  name: string;
  /** Optional durable step name. Defaults to the message name. */
  stepName?: string;
  /** Maximum time to wait. Defaults to 1 year. */
  timeout?: StepWaitTimeout;
  schema?: StandardSchemaV1<unknown, Output>;
}

/**
 * Represents the API for defining steps within a workflow. Used within a
 * workflow handler to define steps by calling `step.run()`, `step.sleep()`,
 * `step.runWorkflow()`, `step.startWorkflow()`, `step.sendSignal()`,
 * `step.waitForSignal()`, `step.sendMessage()`, and `step.waitForMessage()`.
 */
export interface StepApi {
  run: <Output>(
    config: Readonly<StepFunctionConfig>,
    fn: StepFunction<Output>,
  ) => Promise<Output>;
  runWorkflow: <Input, Output, RunInput = Input>(
    spec: WorkflowSpec<Input, Output, RunInput>,
    input?: RunInput,
    options?: Readonly<StepRunWorkflowOptions>,
  ) => Promise<Output>;
  startWorkflow: <Input, Output, RunInput = Input>(
    spec: WorkflowSpec<Input, Output, RunInput>,
    input?: RunInput,
    options?: Readonly<StepStartWorkflowOptions>,
  ) => Promise<string>;
  sleep: (name: string, duration: DurationString) => Promise<void>;
  sendSignal: (
    options: Readonly<{
      name?: string;
      signal: string;
      data?: JsonValue;
    }>,
  ) => Promise<{ workflowRunIds: string[] }>;
  waitForSignal: <Output>(
    options: Readonly<{
      name?: string;
      signal: string;
      timeout?: StepWaitTimeout;
      schema?: StandardSchemaV1<unknown, Output>;
    }>,
  ) => Promise<{ data: Output } | null>;
  sendMessage: (
    options: Readonly<StepSendMessageOptions>,
  ) => Promise<{ messageId: string }>;
  waitForMessage: <Output>(
    options: Readonly<StepWaitForMessageOptions<Output>>,
  ) => Promise<{ data: Output } | null>;
}

/**
 * Read-only workflow run metadata exposed to workflow functions.
 */
export type WorkflowRunMetadata = Pick<
  WorkflowRun,
  "id" | "workflowName" | "createdAt" | "startedAt"
>;

/**
 * Params passed to a workflow function for the user to use when defining
 * steps.
 */
export interface WorkflowFunctionParams<Input> {
  input: Input;
  step: StepApi;
  version: string | null;
  run: WorkflowRunMetadata;
}

/**
 * The workflow definition's function (defined by the user) that the user uses
 * to define the workflow's steps.
 */
export type WorkflowFunction<Input, Output> = (
  params: Readonly<WorkflowFunctionParams<Input>>,
) => Promise<Output> | Output;
