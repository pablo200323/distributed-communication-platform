module.exports = {
  root: true,
  extends: '@react-native',
  overrides: [
    {
      files: ['src/testing/**/*.js'],
      env: {
        jest: true,
      },
    },
  ],
};
