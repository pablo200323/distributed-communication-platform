const MESSAGE_SECURITY_MANAGER_METHODS = Object.freeze([
  'hasKey',
  'importKey',
  'encryptMessage',
  'decodeMessage',
]);

function assertMessageSecurityManager(
  securityManager,
  description = 'message security manager',
) {
  if (
    !securityManager ||
    typeof securityManager !== 'object' ||
    Array.isArray(securityManager)
  ) {
    throw new TypeError(`${description} must be an object`);
  }

  for (const methodName of MESSAGE_SECURITY_MANAGER_METHODS) {
    if (typeof securityManager[methodName] !== 'function') {
      throw new TypeError(`${description} must provide ${methodName}()`);
    }
  }

  return securityManager;
}

export {
  MESSAGE_SECURITY_MANAGER_METHODS,
  assertMessageSecurityManager,
};
