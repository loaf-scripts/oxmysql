import type { QueryResponse, QueryType } from '../../types';
import type { UpsertResult } from 'mariadb';

export const parseResponse = (type: QueryType, result: QueryResponse): unknown => {
  switch (type) {
    case 'insert': {
      const insertId = (result as UpsertResult)?.insertId;
      return insertId != null ? Number(insertId) : null;
    }

    case 'update': {
      const affectedRows = (result as UpsertResult)?.affectedRows;
      return affectedRows != null ? Number(affectedRows) : null;
    }

    case 'single':
      return (result as Record<string, unknown>[])?.[0] ?? null;

    case 'scalar': {
      const row = (result as Record<string, unknown>[])?.[0];
      return (row && Object.values(row)[0]) ?? null;
    }

    default:
      return result ?? null;
  }
};
