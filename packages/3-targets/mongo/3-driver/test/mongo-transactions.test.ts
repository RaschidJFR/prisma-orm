import {
  AggregateWireCommand,
  DeleteManyWireCommand,
  DeleteOneWireCommand,
  FindOneAndDeleteWireCommand,
  FindOneAndUpdateWireCommand,
  InsertManyWireCommand,
  InsertOneWireCommand,
  UpdateManyWireCommand,
  UpdateOneWireCommand,
} from '@internal/mongo-wire';
import { type Collection, MongoClient, MongoServerError } from 'mongodb';
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { MongoDriverImpl } from '../src/mongo-driver';

describe('MongoDriver transactions', () => {
  it('uses one session for all commands and closes it after commit', async () => {
    const client = new MongoClient('mongodb://localhost:27017');
    const db = client.db('transactions');
    const session = client.startSession();
    vi.spyOn(client, 'startSession').mockReturnValue(session);
    const commit = vi.spyOn(session, 'commitTransaction');
    const abort = vi.spyOn(session, 'abortTransaction');
    const end = vi.spyOn(session, 'endSession');
    // Stub collection calls, but keep the real session API to check option forwarding.
    const collection = {
      insertOne: vi.fn().mockResolvedValue({ insertedId: 'one' }),
      insertMany: vi.fn().mockResolvedValue({ insertedIds: { 0: 'two' }, insertedCount: 1 }),
      updateOne: vi.fn().mockResolvedValue({}),
      updateMany: vi.fn().mockResolvedValue({}),
      deleteOne: vi.fn().mockResolvedValue({ deletedCount: 1 }),
      deleteMany: vi.fn().mockResolvedValue({ deletedCount: 1 }),
      findOneAndUpdate: vi.fn().mockResolvedValue(null),
      findOneAndDelete: vi.fn().mockResolvedValue(null),
      aggregate: vi.fn(async function* () {}),
    };
    vi.spyOn(db, 'collection').mockReturnValue(collection as unknown as Collection);
    const commands = [
      new InsertOneWireCommand('users', { name: 'Alice' }),
      new InsertManyWireCommand('users', [{ name: 'Bob' }]),
      new UpdateOneWireCommand('users', {}, { $set: { name: 'Carol' } }),
      new UpdateManyWireCommand('users', {}, { $set: { name: 'Carol' } }),
      new DeleteOneWireCommand('users', {}),
      new DeleteManyWireCommand('users', {}),
      new FindOneAndUpdateWireCommand('users', {}, { $set: { name: 'Carol' } }),
      new FindOneAndDeleteWireCommand('users', {}),
      new AggregateWireCommand('users', []),
    ];

    try {
      const result = await MongoDriverImpl.fromDb(db).transaction(async (tx) => {
        for (const command of commands) {
          for await (const _row of tx.execute(command)) {
            // Consume the command so its session options reach the collection.
          }
          const calls = collection[command.kind].mock.calls;
          expect(calls.at(-1)?.at(-1)).toMatchObject({ session });
        }
        // The wrapper must keep the session open for the entire callback.
        expect(end).not.toHaveBeenCalled();
        return 42;
      });

      expect(result).toBe(42);
      expect(commit).toHaveBeenCalledOnce();
      // A successful transaction must not be aborted.
      expect(abort).not.toHaveBeenCalled();
      expect(end).toHaveBeenCalledOnce();
    } finally {
      await client.close();
    }
  });

  it('rolls back and closes the session when the callback throws', async () => {
    const client = new MongoClient('mongodb://localhost:27017');
    const session = client.startSession();
    vi.spyOn(client, 'startSession').mockReturnValue(session);
    const abort = vi.spyOn(session, 'abortTransaction');
    const commit = vi.spyOn(session, 'commitTransaction');
    const end = vi.spyOn(session, 'endSession');
    const failure = new Error('callback failed');

    try {
      // Preserve the original application error while aborting and cleaning up.
      await expect(
        MongoDriverImpl.fromDb(client.db('transactions')).transaction(async () => {
          throw failure;
        }),
      ).rejects.toBe(failure);
      expect(abort).toHaveBeenCalledOnce();
      // A failed callback must never reach commit.
      expect(commit).not.toHaveBeenCalled();
      expect(end).toHaveBeenCalledOnce();
    } finally {
      await client.close();
    }
  });

  it('closes the session when commit fails', async () => {
    const client = new MongoClient('mongodb://localhost:27017');
    const session = client.startSession();
    vi.spyOn(client, 'startSession').mockReturnValue(session);
    // A non-retryable commit failure must still release the session.
    const failure = new MongoServerError({ message: 'commit failed' });
    const commit = vi.spyOn(session, 'commitTransaction').mockRejectedValue(failure);
    const end = vi.spyOn(session, 'endSession');

    try {
      await expect(
        MongoDriverImpl.fromDb(client.db('transactions')).transaction(async () => 42),
      ).rejects.toBe(failure);
      // This error must not cause another commit attempt.
      expect(commit).toHaveBeenCalledOnce();
      expect(end).toHaveBeenCalledOnce();
    } finally {
      await client.close();
    }
  });
});

// Cases adapted from https://github.com/mongodb/specifications/tree/master/source/transactions-convenient-api/tests/unified
describe('MongoDriver transaction retries (replica set)', () => {
  let replSet: MongoMemoryReplSet;
  let client: MongoClient;
  let driver: MongoDriverImpl;
  let collection: Collection<{ _id: number }>;
  const commands: string[] = [];

  beforeAll(async () => {
    replSet = await MongoMemoryReplSet.create({
      replSet: { count: 1, storageEngine: 'wiredTiger' },
      // Enable controlled server failures on this local test replica set only.
      instanceOpts: [{ args: ['--setParameter', 'enableTestCommands=1'] }],
    });
    client = new MongoClient(replSet.getUri(), { monitorCommands: true });
    await client.connect();
    const db = client.db('transaction_retries');
    collection = await db.createCollection<{ _id: number }>('users');
    driver = MongoDriverImpl.fromDb(db);
    // Observe commands sent to MongoDB, not just calls to wrapper methods.
    client.on('commandStarted', ({ commandName }) => {
      if (['insert', 'commitTransaction', 'abortTransaction'].includes(commandName)) {
        commands.push(commandName);
      }
    });
  });

  beforeEach(async () => {
    await collection.deleteMany({});
    commands.length = 0;
  });

  afterEach(async () => {
    // Always disable the injected failure before another test runs.
    await client.db('admin').command({ configureFailPoint: 'failCommand', mode: 'off' });
  });

  afterAll(async () => {
    await client?.close();
    await replSet?.stop();
  });

  it('reruns the callback after a temporary transaction error', async () => {
    // callback-retry.yml: fail the first write, then retry the whole transaction.
    await client.db('admin').command({
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: ['insert'],
        errorCode: 24,
        errorLabels: ['TransientTransactionError'],
      },
    });
    let calls = 0;
    await driver.transaction(async (tx) => {
      calls++;
      for await (const row of tx.execute(new InsertOneWireCommand('users', { _id: 1 }))) {
        expect(row).toMatchObject({ insertedId: 1 });
      }
    });

    // The failed attempt is aborted; only the retried write is persisted.
    expect(calls).toBe(2);
    expect(commands).toEqual(['insert', 'abortTransaction', 'insert', 'commitTransaction']);
    expect(await collection.find().toArray()).toEqual([{ _id: 1 }]);
  });

  it('retries only commit when its result is unknown', async () => {
    // commit-retry-errorLabels.yml: an uncertain commit must not repeat the writes.
    await client.db('admin').command({
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: ['commitTransaction'],
        errorCode: 64,
        errorLabels: ['UnknownTransactionCommitResult'],
      },
    });
    let calls = 0;
    await driver.transaction(async (tx) => {
      calls++;
      for await (const row of tx.execute(new InsertOneWireCommand('users', { _id: 1 }))) {
        expect(row).toMatchObject({ insertedId: 1 });
      }
    });

    // Retrying only commit preserves the callback's single execution.
    expect(calls).toBe(1);
    expect(commands).toEqual(['insert', 'commitTransaction', 'commitTransaction']);
    expect(await collection.find().toArray()).toEqual([{ _id: 1 }]);
  });

  it('rolls back without retrying on a duplicate key', async () => {
    // callback-retry.yml: a non-transient error rolls back the earlier write.
    let calls = 0;
    await expect(
      driver.transaction(async (tx) => {
        calls++;
        for (let i = 0; i < 2; i++) {
          for await (const row of tx.execute(new InsertOneWireCommand('users', { _id: 1 }))) {
            expect(row).toMatchObject({ insertedId: 1 });
          }
        }
      }),
    ).rejects.toMatchObject({ code: 11000 });

    // The second insert fails, and the first insert must be rolled back too.
    expect(calls).toBe(1);
    expect(commands).toEqual(['insert', 'insert', 'abortTransaction']);
    expect(await collection.find().toArray()).toEqual([]);
  });
});
