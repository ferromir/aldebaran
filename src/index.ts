import { MongoClient } from "mongodb";

const DEFAULT_MAX_FAILURES = 3;
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_POLL_MS = 1_000;
const DEFAULT_RETRY_MS = 60_000;

async function defaultGoSleep(durationMs: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, durationMs);
  });
}

export interface Context {
  step<T>(stepId: string, fn: () => Promise<T>): Promise<T>;
  sleep(napId: string, durationMs: number): Promise<void>;
  start<T>(workflowId: string, handler: string, input: T): Promise<boolean>;
}

export type Status = "idle" | "running" | "failed" | "finished" | "aborted";
export type HandlerFn = (ctx: Context, input: unknown) => Promise<void>;

export interface Client {
  start<T>(workflowId: string, handler: string, input: T): Promise<boolean>;
  poll(shouldStop: () => boolean): Promise<void>;
  close(): Promise<void>;
}

type GoSleepFn = (durationMs: number) => Promise<void>;
type NowFn = () => Date;
const defaultNow = () => new Date();

export interface Options {
  goSleep?: GoSleepFn;
  now?: NowFn;
  maxFailures?: number;
  timeoutIntervalMs?: number;
  pollIntervalMs?: number;
  retryIntervalMs?: number;
}

interface Workflow {
  id: string;
  handler: string;
  input: unknown;
  status: Status;
  timeoutAt: Date;
  failures: number;
  steps: { [key: string]: unknown };
  naps: { [key: string]: Date };
}

interface RunData {
  handler: string;
  input: unknown;
  failures: number;
}

export async function makeClient(
  url: string,
  handlers: Map<string, HandlerFn>,
  options?: Options,
): Promise<Client> {
  const client = new MongoClient(url);
  const db = client.db();
  const workflows = db.collection<Workflow>("workflows");
  await workflows.createIndex({ id: 1 }, { unique: true });
  await workflows.createIndex({ status: 1, timeoutAt: 1 });
  const timeoutIntervalMs = options?.timeoutIntervalMs || DEFAULT_TIMEOUT_MS;
  const pollIntervalMs = options?.pollIntervalMs || DEFAULT_POLL_MS;
  const maxFailures = options?.maxFailures || DEFAULT_MAX_FAILURES;
  const retryIntervalMs = options?.retryIntervalMs || DEFAULT_RETRY_MS;
  const goSleep = options?.goSleep || defaultGoSleep;
  const now = options?.now || defaultNow;

  async function insert(
    workflowId: string,
    handler: string,
    input: unknown,
  ): Promise<boolean> {
    try {
      await workflows.insertOne({
        id: workflowId,
        handler,
        input,
        status: "idle",
        timeoutAt: new Date(),
        failures: 0,
        steps: {},
        naps: {},
      });

      return true;
    } catch (error) {
      const e = error as { name: string; code: number };

      // Workflow already started, ignore.
      if (e.name === "MongoServerError" && e.code === 11000) {
        return false;
      }

      throw error;
    }
  }

  async function claim(): Promise<string | undefined> {
    const _now = now();
    const timeoutAt = new Date(_now.getTime() + timeoutIntervalMs);

    const workflow = await workflows.findOneAndUpdate(
      {
        status: { $in: ["idle", "running", "failed"] },
        timeoutAt: { $lt: _now },
      },
      {
        $set: {
          status: "running",
          timeoutAt,
        },
      },
      {
        projection: {
          _id: 0,
          id: 1,
        },
      },
    );

    return workflow?.id;
  }

  async function findOutput(
    workflowId: string,
    stepId: string,
  ): Promise<unknown> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`steps.${stepId}`]: 1,
        },
      },
    );

    if (workflow && workflow.steps) {
      return workflow.steps[stepId];
    }

    return undefined;
  }

  async function findWakeUpAt(
    workflowId: string,
    napId: string,
  ): Promise<Date | undefined> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`naps.${napId}`]: 1,
        },
      },
    );

    if (workflow && workflow.naps) {
      return workflow.naps[napId];
    }

    return undefined;
  }

  async function findRunData(workflowId: string): Promise<RunData | undefined> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          handler: 1,
          input: 1,
          failures: 1,
        },
      },
    );

    if (workflow) {
      return {
        handler: workflow.handler,
        input: workflow.input,
        failures: workflow.failures,
      };
    }

    return undefined;
  }

  async function setAsFinished(workflowId: string): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: { status: "finished" },
      },
    );
  }

  async function updateStatus(
    workflowId: string,
    status: Status,
    timeoutAt: Date,
    failures: number,
    lastError: string,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          status,
          timeoutAt,
          failures,
          lastError,
        },
      },
    );
  }

  async function updateOutput(
    workflowId: string,
    stepId: string,
    output: unknown,
    timeoutAt: Date,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`steps.${stepId}`]: output,
          timeoutAt,
        },
      },
    );
  }

  async function updateWakeUpAt(
    workflowId: string,
    napId: string,
    wakeUpAt: Date,
    timeoutAt: Date,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`naps.${napId}`]: wakeUpAt,
          timeoutAt,
        },
      },
    );
  }

  function makeStep(workflowId: string) {
    return async function <T>(
      stepId: string,
      fn: () => Promise<T>,
    ): Promise<T> {
      let output = await findOutput(workflowId, stepId);

      if (!(output === undefined)) {
        return output as T;
      }

      output = await fn();
      const _now = now();
      const timeoutAt = new Date(_now.getTime() + timeoutIntervalMs);
      await updateOutput(workflowId, stepId, output, timeoutAt);
      return output as T;
    };
  }

  function makeSleep(workflowId: string) {
    return async function (napId: string, ms: number): Promise<void> {
      let wakeUpAt = await findWakeUpAt(workflowId, napId);
      const _now = now();

      if (wakeUpAt) {
        const remainingMs = wakeUpAt.getTime() - _now.getTime();

        if (remainingMs > 0) {
          await goSleep(remainingMs);
        }

        return;
      }

      wakeUpAt = new Date(_now.getTime() + ms);
      const timeoutAt = new Date(wakeUpAt.getTime() + timeoutIntervalMs);
      await updateWakeUpAt(workflowId, napId, wakeUpAt, timeoutAt);
      await goSleep(ms);
    };
  }

  async function run(workflowId: string): Promise<void> {
    const runData = await findRunData(workflowId);

    if (!runData) {
      throw new Error(`workflow not found: ${workflowId}`);
    }

    const fn = handlers.get(runData.handler);

    if (!fn) {
      throw new Error(`handler not found: ${runData.handler}`);
    }

    const ctx: Context = {
      step: makeStep(workflowId),
      sleep: makeSleep(workflowId),
      start,
    };

    try {
      await fn(ctx, runData.input);
    } catch (error) {
      const lastError = JSON.stringify(error);
      const failures = (runData.failures || 0) + 1;
      const status = failures < maxFailures ? "failed" : "aborted";
      const _now = new Date();
      const timeoutAt = new Date(_now.getTime() + retryIntervalMs);
      await updateStatus(workflowId, status, timeoutAt, failures, lastError);
      return;
    }

    await setAsFinished(workflowId);
  }

  async function start<T>(
    workflowId: string,
    handler: string,
    input: T,
  ): Promise<boolean> {
    return insert(workflowId, handler, input);
  }

  async function poll(shouldStop: () => boolean): Promise<void> {
    while (!shouldStop()) {
      const workflowId = await claim();

      if (workflowId) {
        run(workflowId); // Intentionally not awaiting
      } else {
        await goSleep(pollIntervalMs);
      }
    }
  }

  async function close(): Promise<void> {
    await client.close();
  }

  return { start, poll, close };
}
