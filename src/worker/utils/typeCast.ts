import type { FieldInfo, TypeCastNextFunction, TypeCastFunction, TypeCastResult } from 'mariadb';

const BINARY_COLLATION_FLAG = 1 << 7;

/**
 * mariadb-compatible typecasting (mysql-async compatible).
 * Binary BLOBs are returned as number[] (spread from Buffer) for Lua serialization.
 */
export const typeCast: TypeCastFunction = (column: FieldInfo, next: TypeCastNextFunction): TypeCastResult => {
  switch (column.type) {
    case 'DATETIME':
    case 'DATETIME2':
    case 'TIMESTAMP':
    case 'TIMESTAMP2':
    case 'NEWDATE': {
      const value = column.string();
      return value ? new Date(value).getTime() : null;
    }
    case 'DATE': {
      const value = column.string();
      return value ? new Date(value + ' 00:00:00').getTime() : null;
    }
    case 'TINY':
      return column.columnLength === 1 ? column.string() === '1' : next();
    case 'BIT':
      return column.columnLength === 1 ? column.buffer()?.[0] === 1 : (column.buffer()?.[0] ?? null);
    case 'TINY_BLOB':
    case 'MEDIUM_BLOB':
    case 'LONG_BLOB':
    case 'BLOB':
      if (column.flags & BINARY_COLLATION_FLAG) {
        const value = column.buffer();
        if (value === null) return null;
        // number[] spread for Lua compatibility; single cast contained here
        return [...value] as unknown as TypeCastResult;
      }
      return column.string();
    default:
      return next();
  }
};
