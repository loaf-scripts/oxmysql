import { parentPort } from 'worker_threads';

export function print(...args: any[]) {
  parentPort!.postMessage({ action: 'print', data: args });
}

export function scheduleTick() {
  parentPort!.postMessage({ action: 'scheduleTick' });
}

export function triggerFivemEvent(event: string, payload: any) {
  parentPort!.postMessage({ action: 'triggerEvent', data: { event, payload } });
}

export function sendLogQuery(
  invokingResource: string,
  query: string,
  executionTime: number,
  parameters: any[] | undefined,
  slow: boolean
) {
  parentPort!.postMessage({ action: 'logQuery', data: { invokingResource, query, executionTime, parameters, slow } });
}

export function callLogger(level: string, resource: string, message: string, metadata?: any) {
  parentPort!.postMessage({ action: 'callLogger', data: { level, resource, message, metadata } });
}

export function sendResponse(id: number, data: any) {
  parentPort!.postMessage({ action: 'response', id, data });
}
