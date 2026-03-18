import type { PoolConnection } from 'mariadb';
import { scheduleTick } from '../utils/events';
import { sleep } from '../utils/sleep';
import { pool } from './pool';
import type { CFXParameters } from '../../types';

(Symbol as any).dispose ??= Symbol('Symbol.dispose');

export const activeConnections: Record<number, MySql> = {};

export class MySql {
  id: number;
  connection: PoolConnection;
  transaction?: boolean;

  constructor(connection: PoolConnection) {
    if (!connection.threadId) {
      throw new Error('Connection must have a threadId');
    }

    this.id = connection.threadId;
    this.connection = connection;
    activeConnections[this.id] = this;
  }

  async query(query: string, values: CFXParameters = []) {
    scheduleTick();

    return await this.connection.query(query, values);
  }

  async execute(query: string, values: CFXParameters = []) {
    scheduleTick();

    // Use query() (text protocol) to avoid ER_UNSUPPORTED_PS on SELECT/LIMIT queries
    return await this.connection.query(query, values);
  }

  async batch(query: string, values: CFXParameters[]) {
    scheduleTick();

    return await this.connection.batch(query, values);
  }

  beginTransaction() {
    this.transaction = true;
    return this.connection.beginTransaction();
  }

  rollback() {
    delete this.transaction;
    return this.connection.rollback();
  }

  commit() {
    delete this.transaction;
    return this.connection.commit();
  }

  [Symbol.dispose]() {
    if (this.transaction) this.commit();

    delete activeConnections[this.id];
    this.connection.release();
  }
}

export async function getConnection(connectionId?: number) {
  while (!pool) await sleep(0);

  return connectionId ? activeConnections[connectionId] : new MySql(await pool!.getConnection());
}
