import type { TypeCastField, TypeCastNextFunc } from 'mariadb';

const BINARY_COLLATION = 63;

/**
 * mariadb-compatible typecasting (mysql-async compatible).
 */
export function typeCast(column: TypeCastField, next: TypeCastNextFunc): any {
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
      return column.columnLength === 1 ? column.buffer()?.[0] === 1 : column.buffer()?.[0];
    case 'TINY_BLOB':
    case 'MEDIUM_BLOB':
    case 'LONG_BLOB':
    case 'BLOB':
      if (column.collation === BINARY_COLLATION) {
        const value = column.buffer();
        if (value === null) return [value];
        return [...value];
      }
      return column.string();
    default:
      return next();
  }
}
