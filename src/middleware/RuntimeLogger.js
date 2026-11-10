function formatRuntimeTime(date = new Date()) {
  return String(date.getHours()).padStart(2, '0') + ':' + String(date.getMinutes()).padStart(2, '0');
}

function logRuntimeEvent(...values) {
  console.log('[' + formatRuntimeTime() + ']', ...values);
}

function logRuntimeError(...values) {
  console.error('[' + formatRuntimeTime() + ']', ...values);
}

export { formatRuntimeTime, logRuntimeError, logRuntimeEvent };
