import { mysql_debug, mysql_slow_query_warning, mysql_ui } from '../config';
import type { CFXParameters } from '../../types';
import { dbVersion } from '../database/pool';
import { print, sendLogQuery, triggerFivemEvent, callLogger } from '../utils/events';

export function logError(
  invokingResource: string,
  err: any,
  query?: string,
  parameters?: CFXParameters,
  includeParameters?: boolean
): string {
  const message =
    typeof err === 'object' ? err.message : String(err).replace(/SCRIPT ERROR: citizen:[\w\/\.]+:\d+[:\s]+/, '');

  const output = `${invokingResource} was unable to execute a query!${query ? `\nQuery: ${query}` : ''}${
    includeParameters ? `\n${JSON.stringify(parameters)}` : ''
  }\n${message}`;

  triggerFivemEvent('oxmysql:error', {
    query,
    parameters,
    message,
    err,
    resource: invokingResource,
  });

  if (typeof err === 'object' && err.message) delete err.sqlMessage;

  callLogger('error', invokingResource, message, err);

  return output;
}

export function logQuery(invokingResource: string, query: string, executionTime: number, parameters?: CFXParameters) {
  const isSlow = executionTime >= mysql_slow_query_warning;
  const isDebug = mysql_debug && (!Array.isArray(mysql_debug) || mysql_debug.includes(invokingResource));

  if (isSlow || isDebug) {
    print(
      `${dbVersion} ^3${invokingResource} took ${executionTime.toFixed(4)}ms to execute a query!\n${query}${
        parameters ? ` ${JSON.stringify(parameters)}` : ''
      }^0`
    );
  }

  if (!mysql_ui) return;

  sendLogQuery(invokingResource, query, executionTime, parameters, isSlow);
}

export function logger(data: { level: string; resource: string; message: string; metadata?: any }) {
  callLogger(data.level, data.resource, data.message, data.metadata);
}
