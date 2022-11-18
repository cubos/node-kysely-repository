import { randomBytes } from "crypto";

import { Kysely, PostgresDialect, sql } from "kysely";
import type { PoolConfig } from "pg";
import { Pool } from "pg";

import { BaseRepository } from "../src";
import type { BaseModel } from "../src/BaseRepository";

const poolConfig: PoolConfig = {
  database: "postgres",
  host: process.env.DB_HOST ?? "localhost",
  password: process.env.DB_PASSWORD ?? "postgres",
  user: process.env.DB_USER ?? "postgres",
};

type Database = Record<string, BaseModel>;

function randomTableName() {
  return `test${randomBytes(8).toString("hex")}`;
}

describe("BaseRepository", () => {
  const database = `test_${randomBytes(8).toString("hex")}`;
  const masterConn = new Kysely<Database>({ dialect: new PostgresDialect({ pool: new Pool(poolConfig) }) });
  const conn = new Kysely<Database>({ dialect: new PostgresDialect({ pool: new Pool({ ...poolConfig, database }) }) });

  beforeAll(async () => {
    await sql`CREATE DATABASE ${sql.raw(database)}`.execute(masterConn);
  });

  afterAll(async () => {
    await conn.destroy();
    await sql`DROP DATABASE ${sql.raw(database)}`.execute(masterConn);
    await masterConn.destroy();
  });

  it("inserts, finds, updates and deletes objects", async () => {
    const tableName = randomTableName();

    await BaseRepository.createTable(conn, tableName, table => {
      return table.addColumn("name", "text");
    });

    interface Model extends BaseModel {
      name: string;
    }

    type TestDb = Record<string, Model>;

    const repo = new BaseRepository<TestDb, string>(conn as Kysely<TestDb>, tableName);

    expect(await repo.findAll()).toHaveLength(0);

    const inserted = await repo.insert({ name: "foo" });

    expect(inserted.name).toBe("foo");
    expect(inserted.createdAt).toBeInstanceOf(Date);
    expect(inserted.updatedAt).toBeInstanceOf(Date);
    expect(inserted.updatedAt.getTime()).toBe(inserted.updatedAt.getTime());
    expect(inserted.id).toBeDefined();
    expect(await repo.findAll()).toEqual([inserted]);

    const updated = await repo.update({ id: inserted.id, name: "bar" });

    expect(updated.name).toBe("bar");
    expect(updated.id).toBe(inserted.id);
    expect(updated.createdAt.getTime()).toBe(inserted.createdAt.getTime());
    expect(updated.updatedAt.getTime()).toBeGreaterThan(inserted.updatedAt.getTime());
    expect(await repo.findAll()).toEqual([updated]);
    expect(await repo.findBy({ name: "bar" })).toEqual([updated]);
    expect(await repo.findOneBy({ name: "bar" })).toEqual(updated);
    expect(await repo.get(inserted.id)).toEqual(updated);
    expect(await repo.findBy({ name: "foo" })).toEqual([]);
    expect(await repo.count()).toBe(1);

    const deleted = await repo.delete(updated.id);

    expect(updated).toEqual(deleted);
    expect(await repo.findAll()).toEqual([]);
    expect(await repo.count()).toBe(0);

    await expect(repo.update({ id: inserted.id, name: "baz" })).rejects.toThrowError("no result");
    await expect(repo.delete(updated.id)).rejects.toThrowError("no result");
  });

  it("allows altering tables", async () => {
    const tableName = randomTableName();

    await BaseRepository.createTable(conn, tableName, table => {
      return table.addColumn("name", "text");
    });

    interface Model1 extends BaseModel {
      name: string;
    }

    type TestDb1 = Record<string, Model1>;

    await new BaseRepository<TestDb1, string>(conn as Kysely<TestDb1>, tableName).insert({ name: "foo" });

    await BaseRepository.alterTable(conn, tableName, table => {
      return table.addColumn("age", "integer").defaultTo(20);
    });

    interface Model2 extends BaseModel {
      name: string;
      age: number;
    }

    type TestDb2 = Record<string, Model2>;

    const [row] = await new BaseRepository<TestDb2, string>(conn as Kysely<TestDb2>, tableName).findAll();

    expect(row.name).toBe("foo");
    expect(row.age).toBe(20);

    await BaseRepository.dropTable(conn, tableName);
  });

  it("inserts and deletes many", async () => {
    const tableName = randomTableName();

    await BaseRepository.createTable(conn, tableName, table => {
      return table.addColumn("value", "integer");
    });

    interface Model extends BaseModel {
      value: number;
    }

    type TestDb = Record<string, Model>;

    const repo = new BaseRepository<TestDb, string>(conn as Kysely<TestDb>, tableName);

    const objectsToInsert = new Array(10).fill(0).map((_, index) => ({ value: index }));

    const insertedObjects = await repo.insertAll(objectsToInsert);

    expect(insertedObjects.map(x => x.value)).toEqual(objectsToInsert.map(x => x.value));
    expect(new Set(insertedObjects.map(x => x.id)).size).toEqual(objectsToInsert.length);
    expect(new Set(insertedObjects.map(x => x.createdAt.getTime())).size).toEqual(1);
    expect(new Set(insertedObjects.map(x => x.updatedAt.getTime())).size).toEqual(1);

    const deletedObjects = await repo.deleteBy(item => item.where("value", ">=", 5));

    expect(deletedObjects).toEqual(insertedObjects.slice(5));
    expect(await repo.findAll()).toEqual(expect.arrayContaining(insertedObjects.slice(0, 5)));

    const moreDeletedObjects = await repo.deleteBy({ value: 0 });

    expect(moreDeletedObjects).toEqual([insertedObjects[0]]);
    expect(await repo.findAll()).toEqual(expect.arrayContaining(insertedObjects.slice(1, 5)));

    await repo.truncate();

    expect(await repo.findAll()).toEqual([]);
  });
});
