import { createPool } from 'mariadb';
import type { Pool, PoolConfig } from 'mariadb';
import { mysql_transaction_isolation_level } from '../config';
import { typeCast } from '../utils/typeCast';
import { print } from '../utils/events';
import { parentPort } from 'worker_threads';

export let pool: Pool | null = null;
export let dbVersion = '';

export async function createConnectionPool(options: PoolConfig) {
  try {
    const dbPool = createPool({ ...options, typeCast, initSql: mysql_transaction_isolation_level });

    const result = await dbPool.query<Array<{ version: string }>>('SELECT VERSION() as version');
    dbVersion = `^5[${result[0].version}]`;

    print(`${dbVersion} ^2Database server connection established!^0`);
    parentPort!.postMessage({ action: 'dbVersion', data: dbVersion });

    if (options.multipleStatements) {
      print(`multipleStatements is enabled. Used incorrectly, this option may cause SQL injection.`);
    }

    pool = dbPool;
  } catch (err) {
    const error = err as { message?: string; code?: string; errno?: number };
    const message = error.message?.includes('auth_gssapi_client')
      ? `Requested authentication using unknown plugin auth_gssapi_client.`
      : error.message;

    print(
      `^3Unable to establish a connection to the database (${error.code})!\n^1Error${
        error.errno ? ` ${error.errno}` : ''
      }: ${message}^0`
    );

    print(`See https://github.com/overextended/oxmysql/issues/154 for more information.`);

    if ((options as Record<string, unknown>).password) (options as Record<string, unknown>).password = '******';
    print(JSON.stringify(options));
  }
}
