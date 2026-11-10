module.exports = {
  preset: 'react-native',
  testEnvironment: 'node',
  testMatch: ['**/src/testing/unit/**/*.js'],
  testPathIgnorePatterns: ['\\\\node_modules\\\\', '<rootDir>/.worktree/'],
  modulePathIgnorePatterns: ['<rootDir>/.worktree/'],
  moduleFileExtensions: ['js', 'jsx', 'ts', 'tsx', 'json'],
  clearMocks: true,
  resetMocks: false,
  restoreMocks: true,
};
