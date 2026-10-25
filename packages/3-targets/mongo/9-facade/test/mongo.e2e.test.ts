import { timeouts } from '@repo/test-utils';
import { MongoClient } from 'mongodb';
import { MongoMemoryReplSet, MongoMemoryServer } from 'mongodb-memory-server';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Contract } from '../../../../2-mongo-family/1-foundation/mongo-contract/test/fixtures/orm-contract';
import contractJson from '../../../../2-mongo-family/1-foundation/mongo-contract/test/fixtures/orm-contract.json' with {
  type: 'json',
};
import mongo, { type MongoTransactionContext } from '../src/runtime/mongo';
import { contractFromJson } from '../src/static/contract-from-json';

const contract = contractFromJson<Contract>(contractJson);

// End-to-end smoke test for the lazy mongo() facade. Exercises the full
// orm → executor → runtime → driver chain against a real (in-memory) replica
// set.
//
// `MongoMemoryReplSet` (not `MongoMemoryServer`) is deliberate: FR4.4
// documents that transactions and change streams require a replica set, so
// the e2e environment matches the production story we point users at.
//
// We use `{ uri, dbName }` rather than `{ url }` because `MongoMemoryReplSet`
// returns URIs with a `?replicaSet=...` query string, which makes path-based
// dbName injection ambiguous.
describe('mongo() facade — e2e (replica set)', {
  timeout: timeouts.spinUpMongoMemoryServer,
}, () => {
  let replSet: MongoMemoryReplSet;

  beforeAll(async () => {
    replSet = await MongoMemoryReplSet.create({
      replSet: { count: 1, storageEngine: 'wiredTiger' },
    });
  }, timeouts.spinUpMongoMemoryServer);

  afterAll(async () => {
    await replSet?.stop();
  }, timeouts.spinUpMongoMemoryServer);

  it('orm and query are available before any connection is made', () => {
    const db = mongo({
      contract,
      uri: replSet.getUri(),
      dbName: 'facade_eager',
    });
    expect(db.orm).toBeDefined();
    expect(db.orm.users).toBeDefined();
    expect(db.orm.tasks).toBeDefined();
    expect(db.query).toBeDefined();
  });

  it(
    'createAll + where().first() round-trips through the lazy facade',
    async () => {
      const db = mongo({
        contract,
        uri: replSet.getUri(),
        dbName: 'facade_first',
      });

      try {
        const created = await db.orm.users.createAll([
          {
            name: 'Alice',
            email: 'alice@example.com',
            loginCount: 1,
            tags: ['admin'],
            homeAddress: null,
          },
          {
            name: 'Bob',
            email: 'bob@example.com',
            loginCount: 0,
            tags: [],
            homeAddress: null,
          },
        ]);

        expect(created).toHaveLength(2);
        expect(created[0]?._id).toBeTruthy();

        const alice = await db.orm.users.where({ email: 'alice@example.com' }).first();
        expect(alice?.name).toBe('Alice');
        expect(alice?.email).toBe('alice@example.com');
      } finally {
        await db.close();
      }
    },
    timeouts.spinUpMongoMemoryServer,
  );

  it(
    'connect() is callable explicitly and drives the same lazy runtime',
    async () => {
      const db = mongo({ contract });
      try {
        await db.connect({ uri: replSet.getUri(), dbName: 'facade_explicit' });
        const users = await db.orm.users.all();
        expect([...users]).toHaveLength(0);
      } finally {
        await db.close();
      }
    },
    timeouts.spinUpMongoMemoryServer,
  );

  it(
    'connect() rejects when called twice',
    async () => {
      const db = mongo({
        contract,
        uri: replSet.getUri(),
        dbName: 'facade_twice',
      });
      try {
        await db.connect();
        await expect(db.connect({ uri: replSet.getUri(), dbName: 'facade_twice' })).rejects.toThrow(
          'Mongo client already connected',
        );
      } finally {
        await db.close();
      }
    },
    timeouts.spinUpMongoMemoryServer,
  );

  it.each(['owned', 'borrowed'])(
    'commits writes and isolates uncommitted reads (%s client)',
    async (ownership) => {
      const client = new MongoClient(replSet.getUri());
      const db = mongo({
        contract,
        ...(ownership === 'owned' ? { uri: replSet.getUri() } : { mongoClient: client }),
        dbName: `transaction_commit_${ownership}`,
      });
      try {
        const result = await db.transaction(async (tx) => {
          expect(tx).not.toHaveProperty('transaction');
          const alice = await tx.orm.users.create({
            name: 'Alice',
            email: 'alice@example.com',
            loginCount: 0,
            tags: [],
            homeAddress: null,
          });
          await tx.orm.users.createAll([
            {
              name: 'Bob',
              email: 'bob@example.com',
              loginCount: 0,
              tags: [],
              homeAddress: null,
            },
          ]);
          // The transaction can read its own writes before commit.
          expect(await tx.orm.users.where({ email: alice.email }).first()).toEqual(alice);
          // The normal client must not read the session's uncommitted writes.
          expect(await db.orm.users.all()).toEqual([]);
          return 'committed';
        });
        expect(result).toBe('committed');
        expect((await db.orm.users.all()).map(({ name }) => name).sort()).toEqual(['Alice', 'Bob']);
        if (ownership === 'borrowed') {
          // Closing the facade must not close a caller-owned MongoClient.
          await db.close();
          expect(
            await client.db(`transaction_commit_${ownership}`).command({ ping: 1 }),
          ).toMatchObject({ ok: 1 });
        }
      } finally {
        await db.close();
        await client.close();
      }
    },
  );

  it('rolls back writes and invalidates the transaction on callback failure', async () => {
    const db = mongo({
      contract,
      uri: replSet.getUri(),
      dbName: 'transaction_abort',
    });
    let escaped: MongoTransactionContext<Contract> | undefined;
    const failure = new Error('cancel transaction');
    try {
      await expect(
        db.transaction(async (tx) => {
          escaped = tx;
          await tx.orm.users.create({
            name: 'Alice',
            email: 'alice@example.com',
            loginCount: 0,
            tags: [],
            homeAddress: null,
          });
          throw failure;
        }),
      ).rejects.toBe(failure);
      // The write is rolled back, and the escaped transaction cannot be reused.
      expect(await db.orm.users.all()).toEqual([]);
      await expect(escaped!.orm.users.all().toArray()).rejects.toThrow('transaction has ended');
    } finally {
      await db.close();
    }
  });

  it('rejects transactions after close without connecting', async () => {
    // No connection binding is supplied: the closed-client guard must run first.
    const db = mongo({ contract });
    await db.close();
    await expect(db.transaction(async () => 1)).rejects.toThrow('Mongo client is closed');
  });

  it('rejects transactions on standalone MongoDB', async () => {
    const server = await MongoMemoryServer.create();
    const db = mongo({ contract, uri: server.getUri(), dbName: 'standalone' });
    try {
      await expect(
        db.transaction(async (tx) => {
          // MongoDB starts the server-side transaction on its first command.
          await tx.orm.users.all();
        }),
      ).rejects.toThrow(/replica set|mongos/i);
      expect(await db.orm.users.all()).toEqual([]);
    } finally {
      await db.close();
      await server.stop();
    }
  });
});
