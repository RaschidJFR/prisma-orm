import type { AsyncIterableResult } from '@internal/framework-components/runtime';
import type { MongoDriver } from '@internal/mongo-lowering';
import { DeleteOneWireCommand } from '@internal/mongo-wire';
import { describe, expect, it, vi } from 'vitest';
import { createMongoRuntime, type MongoTransaction } from '../src/mongo-runtime';
import {
  createMockAdapter,
  createMockDriver,
  createPlan,
  makeContext,
} from './mongo-middleware-fixtures';

function setup() {
  // Separate drivers reveal any operation that bypasses the transaction.
  const scopedDriver = createMockDriver([{ deletedCount: 1 }, { deletedCount: 2 }]);
  const driver = {
    ...createMockDriver(),
    async transaction<R>(fn: (tx: Pick<MongoDriver, 'execute'>) => Promise<R>): Promise<R> {
      return fn(scopedDriver);
    },
  };
  const observations: { id: string; scope: string }[] = [];
  const runtime = createMongoRuntime({
    context: makeContext(createMockAdapter(), new DeleteOneWireCommand('users', {})),
    driver,
    middleware: [
      {
        name: 'observer',
        async activityStarted(activity, ctx) {
          observations.push({ id: activity.id, scope: ctx.scope });
        },
      },
    ],
  });
  return { runtime, driver, scopedDriver, observations };
}

describe('MongoRuntime transactions', () => {
  it('uses the transaction driver for queries and mutations', async () => {
    const { runtime, driver, scopedDriver, observations } = setup();
    vi.mocked(scopedDriver.execute).mockImplementation(async function* <Row>() {
      yield { deletedCount: 1 } as Row;
    });

    const result = await runtime.transaction(async (tx) => {
      // Both execution paths must use the transaction; nesting is not exposed.
      expect(tx).not.toHaveProperty('transaction');
      expect(await tx.query(createPlan())).toEqual([{ deletedCount: 1 }]);
      expect(await tx.execute(createPlan())).toEqual({ affectedRows: 1 });
      return 42;
    });

    expect(result).toBe(42);
    expect(driver.execute).not.toHaveBeenCalled();
    expect(scopedDriver.execute).toHaveBeenCalledTimes(2);
    // Statements share transaction scope, but retain separate middleware identities.
    expect(observations.map(({ scope }) => scope)).toEqual(['transaction', 'transaction']);
    expect(new Set(observations.map(({ id }) => id)).size).toBe(2);
  });

  it.each(['commit', 'abort'])('rejects queries and mutations after %s', async (outcome) => {
    const { runtime, scopedDriver } = setup();
    let escaped: MongoTransaction | undefined;
    let rows: AsyncIterableResult<unknown> | undefined;
    let iterator: AsyncIterator<unknown> | undefined;
    const failure = new Error('abort');
    const transaction = runtime.transaction(async (tx) => {
      escaped = tx;
      // Keep both an unstarted query and a partially consumed cursor past the callback.
      rows = tx.query(createPlan());
      iterator = tx.query(createPlan())[Symbol.asyncIterator]();
      await iterator.next();
      if (outcome === 'abort') throw failure;
    });
    if (outcome === 'abort') await expect(transaction).rejects.toBe(failure);
    else await transaction;

    // Every escaped execution path must fail without another driver call.
    const calls = vi.mocked(scopedDriver.execute).mock.calls.length;
    await expect(rows!.toArray()).rejects.toThrow('transaction has ended');
    await expect(iterator!.next()).rejects.toThrow('transaction has ended');
    await expect(escaped!.query(createPlan()).toArray()).rejects.toThrow('transaction has ended');
    await expect(escaped!.execute(createPlan())).rejects.toThrow('transaction has ended');
    expect(scopedDriver.execute).toHaveBeenCalledTimes(calls);
  });

  it('does not send commands for an aborted operation', async () => {
    const { runtime, scopedDriver } = setup();
    // Cancellation must still be checked before a transaction command is sent.
    const signal = AbortSignal.abort();
    await expect(
      runtime.transaction(async (tx) => {
        await tx.query(createPlan(), { signal });
      }),
    ).rejects.toMatchObject({ code: 'RUNTIME.ABORTED' });
    expect(scopedDriver.execute).not.toHaveBeenCalled();
  });
});
