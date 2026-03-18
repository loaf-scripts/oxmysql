import { print } from './events';

const oversizedResultSet = 1000;

export default function validateResultSet(invokingResource: string, query: string, rows: any) {
  const length = Array.isArray(rows) ? rows.length : 0;

  if (length < oversizedResultSet) return;

  print(`${invokingResource} executed a query with an oversized result set (${length} results)!\n${query}`);
}
