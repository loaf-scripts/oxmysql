export let mysql_debug: boolean | string[] = false;
export let mysql_slow_query_warning = 200;
export let mysql_ui = false;
export let mysql_log_size = 100;
export let mysql_transaction_isolation_level = 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED';
export let convertNamedPlaceholders:
  | null
  | ((query: string, parameters: Record<string, unknown>) => [string, unknown[]]) = null;

export function updateConfig(config: {
  mysql_debug: boolean | string[];
  mysql_slow_query_warning: number;
  mysql_ui: boolean;
  mysql_log_size: number;
}) {
  mysql_debug = config.mysql_debug;
  mysql_slow_query_warning = config.mysql_slow_query_warning;
  mysql_ui = config.mysql_ui;
  mysql_log_size = config.mysql_log_size;
}

export function setIsolationLevel(level: string) {
  mysql_transaction_isolation_level = level;
}

export function initNamedPlaceholders(optionValue: unknown) {
  // Only disable if the user explicitly wrote namedPlaceholders=false (string) in their
  // connection string. Boolean false is our own internal pool override and should not
  // disable named-placeholder conversion.
  convertNamedPlaceholders = optionValue === 'false' ? null : require('named-placeholders')();
}
