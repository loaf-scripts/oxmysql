import { parentPort } from 'worker_threads';
import { updateConfig, setIsolationLevel, initNamedPlaceholders } from './config';
import { createConnectionPool, pool } from './database/pool';
import { rawQuery } from './database/rawQuery';
import { rawExecute } from './database/rawExecute';
import { rawTransaction } from './database/rawTransaction';
import { beginTransaction, runTransactionQuery, endTransaction } from './database/startTransaction';
import { sendResponse } from './utils/events';
import { sleep } from './utils/sleep';
import type { QueryType, TransactionQuery, CFXParameters } from '../types';

parentPort!.on('message', async (message: { action: string; id?: number; data: any }) => {
  const { action, id, data } = message;

  switch (action) {
    case 'initialize': {
      const { connectionOptions, mysql_transaction_isolation_level, mysql_debug, namedPlaceholders } = data;

      setIsolationLevel(mysql_transaction_isolation_level);
      // Use the user's original value, not connectionOptions.namedPlaceholders which is
      // always boolean false (set to disable mariadb's own handling in favour of ours).
      initNamedPlaceholders(namedPlaceholders);

      updateConfig({
        mysql_debug,
        mysql_slow_query_warning: 200,
        mysql_ui: false,
        mysql_log_size: 100,
      });

      // Retry pool creation until successful
      while (!pool) {
        await createConnectionPool(connectionOptions);
        if (!pool) await sleep(30000);
      }

      break;
    }

    case 'updateConfig': {
      updateConfig(data);
      break;
    }

    case 'query': {
      const { type, invokingResource, query, parameters } = data as {
        type: QueryType;
        invokingResource: string;
        query: string;
        parameters: CFXParameters;
      };

      const result = await rawQuery(type, invokingResource, query, parameters);
      sendResponse(id!, result);
      break;
    }

    case 'execute': {
      const { invokingResource, query, parameters, unpack } = data as {
        invokingResource: string;
        query: string;
        parameters: CFXParameters;
        unpack?: boolean;
      };

      const result = await rawExecute(invokingResource, query, parameters, unpack);
      sendResponse(id!, result);
      break;
    }

    case 'transaction': {
      const { invokingResource, queries, parameters } = data as {
        invokingResource: string;
        queries: TransactionQuery;
        parameters: CFXParameters;
      };

      const result = await rawTransaction(invokingResource, queries, parameters);
      sendResponse(id!, result);
      break;
    }

    case 'beginTransaction': {
      const { invokingResource } = data as { invokingResource: string };
      const result = await beginTransaction(invokingResource);
      sendResponse(id!, result);
      break;
    }

    case 'transactionQuery': {
      const { invokingResource, connectionId, sql, values } = data as {
        invokingResource: string;
        connectionId: number;
        sql: string;
        values: CFXParameters;
      };

      const result = await runTransactionQuery(invokingResource, connectionId, sql, values);
      sendResponse(id!, result);
      break;
    }

    case 'endTransaction': {
      const { connectionId, commit } = data as { connectionId: number; commit: boolean };
      await endTransaction(connectionId, commit);
      break;
    }
  }
});
