import { randomUUID } from "crypto";

import type { AlterTableBuilder, AlterTableExecutor, CreateTableBuilder, InsertObject, Kysely, SelectQueryBuilder, Transaction } from "kysely";
import { sql } from "kysely";

export interface BaseModel {
  id: string;
  createdAt: Date;
  updatedAt: Date;
}

type Insert<T extends BaseModel> = Omit<T, "createdAt" | "updatedAt" | "id"> & { id?: T["id"] };
type Filter<T extends BaseModel> = Partial<Omit<T, "createdAt" | "updatedAt">>;
type Update<T extends BaseModel> = Filter<T> & { id: T["id"] };

export class BaseRepository<Database extends Record<keyof Database, BaseModel>, TableName extends keyof Database & string> {
  constructor(private readonly db: Kysely<Pick<Database, TableName>>, private readonly tableName: TableName) {}

  static async createTable(
    db: Kysely<unknown>,
    tableName: string,
    tableBuilder: (table: CreateTableBuilder<string, string>) => CreateTableBuilder<string, string>,
    idColumnType: "text" | "uuid" = "uuid",
  ) {
    let op = db.schema
      .createTable(tableName)
      .addColumn("id", idColumnType, col => col.primaryKey())
      .addColumn("createdAt", "timestamptz", col => col.notNull().defaultTo(sql`NOW()`))
      .addColumn("updatedAt", "timestamptz", col => col.notNull().defaultTo(sql`NOW()`));

    op = tableBuilder(op);

    await op.execute();
  }

  static async dropTable(db: Kysely<unknown>, tableName: string) {
    await db.schema.dropTable(tableName).execute();
  }

  static async alterTable(
    db: Kysely<unknown>,
    tableName: string,
    tableBuilder: (tableBuilder: AlterTableBuilder) => Omit<AlterTableExecutor, "#private">,
  ) {
    const op = db.schema.alterTable(tableName);
    const res = tableBuilder(op);

    await res.execute();
  }

  withTransaction(db: Transaction<Pick<Database, TableName>>) {
    return new BaseRepository<Database, TableName>(db, this.tableName);
  }

  private select() {
    return this.db.selectFrom(this.tableName);
  }

  /**
   * Insere um objeto e retorna a instância criada
   *
   * @param item objeto a ser inserido
   * @returns instância do objeto criado
   */
  async insert(item: Insert<Database[TableName]>) {
    const now = new Date();

    const [result] = await this.db
      .insertInto(this.tableName)
      .values({
        id: randomUUID(),
        ...item,
        createdAt: now,
        updatedAt: now,
      } as InsertObject<Pick<Database, TableName>, TableName>)
      .returningAll()
      .execute();

    return result;
  }

  /**
   * Insere múltiplos objetos e retorna as instâncias criadas
   *
   * @param items objetos a serem inseridos
   * @returns instância dos objetos criados
   */
  async insertAll(items: Array<Insert<Database[TableName]>>) {
    const now = new Date();

    return this.db
      .insertInto(this.tableName)
      .values(
        items.map<any>(item => ({
          id: randomUUID(),
          ...item,
          createdAt: now,
          updatedAt: now,
        })),
      )
      .returningAll()
      .execute();
  }

  /**
   * Obtém a última versão de um objeto através de parâmetro(s) do mesmo
   *
   * @param condition condição de busca através de parâmetros ou com um callback do knex
   * @returns instância do objeto ou undefined se não encontrado
   */
  async findOneBy(
    condition:
      | Filter<Database[TableName]>
      | ((qb: SelectQueryBuilder<Database, TableName, Database[TableName]>) => SelectQueryBuilder<Database, TableName, Database[TableName]>) = {},
  ) {
    let op = this.select().selectAll();

    if (typeof condition === "function") {
      op = condition(op as any) as any;
    } else {
      for (const [key, value] of Object.entries(condition)) {
        op = op.where(key as any, "=", value);
      }
    }

    return op.executeTakeFirst();
  }

  /**
   * Obtém uma sequência de objetos de acordo com um limite e pagina de busca
   *
   * @param page página na qual deseja-se realizar a busca
   * @param pageSize limite de itens retornados pela busca
   * @param condition parâmetros dos objetos a serem utilizadas na condição de busca
   * @param queryBuilder callback síncrono possibilitando adicionar mais parâmetros na condição de busca
   * @returns objeto contendo o resultado e configurações da pesquisa
   */
  async findAllPaginated(
    page = 1,
    pageSize = 10,
    condition:
      | Filter<Database[TableName]>
      | ((qb: SelectQueryBuilder<Database, TableName, Database[TableName]>) => SelectQueryBuilder<Database, TableName, Database[TableName]>) = {},
  ) {
    const rowCount = await this.count(condition);
    let op = this.select().selectAll();

    if (typeof condition === "function") {
      op = condition(op as any) as any;
    } else {
      for (const [key, value] of Object.entries(condition)) {
        op = op.where(key as any, "=", value);
      }
    }

    const result = await op
      .limit(pageSize)
      .offset((page - 1) * pageSize)
      .execute();

    return {
      data: result,
      page,
      pageCount: Math.ceil(rowCount / pageSize),
      pageSize,
      rowCount,
    };
  }

  /**
   * Obtém a contagem de objetos através de parâmetro(s) do mesmo
   *
   * @param condition condição de busca através de parâmetros ou com um callback do knex
   * @returns contagem de objetos
   */
  async count(
    condition:
      | Filter<Database[TableName]>
      | ((qb: SelectQueryBuilder<Database, TableName, Database[TableName]>) => SelectQueryBuilder<Database, TableName, Database[TableName]>) = {},
  ) {
    let op = this.select();

    if (typeof condition === "function") {
      op = condition(op as any) as any;
    } else {
      for (const [key, value] of Object.entries(condition)) {
        op = op.where(key as any, "=", value);
      }
    }

    op = op.select(sql`COUNT(*) AS count` as any);

    const { count } = (await op.executeTakeFirst()) as { count: string | undefined };

    return parseInt((count ?? "0").toString(), 10);
  }

  /**
   * Obtém a última versão de todos os objetos
   *
   * @returns array com a instância dos objetos
   */
  async findAll() {
    return this.select().selectAll().execute();
  }

  /**
   * Obtém a última versão de alguns objetos através de parâmetro(s) dos mesmos
   *
   * @param condition condição de busca através de parâmetros ou com um callback do knex
   * @returns array com a instância dos objetos encontrados
   */
  async findBy(
    condition:
      | Filter<Database[TableName]>
      | ((qb: SelectQueryBuilder<Database, TableName, Database[TableName]>) => SelectQueryBuilder<Database, TableName, Database[TableName]>),
  ) {
    let op = this.select().selectAll();

    if (typeof condition === "function") {
      op = condition(op as any) as any;
    } else {
      for (const [key, value] of Object.entries(condition)) {
        op = op.where(key as any, "=", value);
      }
    }

    return op.execute();
  }

  /**
   * Obtém a última versão de um objeto através do identificador
   *
   * @param id identificador do objeto
   * @returns instância do objeto ou undefined se não encontrado
   */
  async get(id: Database[TableName]["id"]) {
    return this.select()
      .selectAll()
      .where("id", "=", id as any)
      .executeTakeFirst();
  }

  /**
   * Atualiza uma instância de um objeto
   *
   * @param item objeto a ser atualizado
   * @returns  objeto atualizado
   * @throws NoResultError
   */
  async update(item: Update<Database[TableName]>) {
    const updatedItem = await this.db
      .updateTable(this.tableName)
      .where("id", "=", item.id as any)
      .set({
        ...item,
        updatedAt: new Date(),
      } as any)
      .returningAll()
      .executeTakeFirstOrThrow();

    return updatedItem;
  }

  /**
   * Exclui a instância de um objeto através do identificador
   *
   * @param id identificador do objeto
   * @returns objeto excluído
   * @throws NoResultError
   */
  async delete(id: Database[TableName]["id"]) {
    return this.db
      .deleteFrom(this.tableName)
      .where("id", "=", id as any)
      .returningAll()
      .executeTakeFirstOrThrow();
  }

  /**
   * Exclui múltiplas instâncias de um objeto através de uma condição
   *
   * @param condition condição de busca através de parâmetros ou com um callback do knex
   * @returns objetos excluídos
   */
  async deleteBy(
    condition:
      | Filter<Database[TableName]>
      | ((qb: SelectQueryBuilder<Database, TableName, Database[TableName]>) => SelectQueryBuilder<Database, TableName, Database[TableName]>),
  ) {
    let op = this.db.deleteFrom(this.tableName);

    if (typeof condition === "function") {
      op = condition(op as any) as any;
    } else {
      for (const [key, value] of Object.entries(condition)) {
        op = op.where(key as any, "=", value);
      }
    }

    return op.returningAll().execute();
  }

  /**
   * Exclui todos os objetos da tabela.
   */
  async truncate() {
    await this.db.deleteFrom(this.tableName).execute();
  }
}
