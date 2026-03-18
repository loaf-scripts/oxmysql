import type { QueryResponse, QueryType } from '../../types';

export const parseResponse = (type: QueryType, result: QueryResponse): any => {
  switch (type) {
    case 'insert': {
      const insertId = (result as any)?.insertId;
      return insertId != null ? Number(insertId) : null;
    }

    case 'update': {
      const affectedRows = (result as any)?.affectedRows;
      return affectedRows != null ? Number(affectedRows) : null;
    }

    case 'single':
      return (result as any[])?.[0] ?? null;

    case 'scalar': {
      const row = (result as any[])?.[0];
      return (row && Object.values(row)[0]) ?? null;
    }

    default:
      return result ?? null;
  }
};
