module.exports = {
  rules: {
    '@typescript-eslint/consistent-type-assertions': 'error',
    '@typescript-eslint/no-non-null-assertion': 'error',
    '@typescript-eslint/explicit-function-return-type': 'error',
    '@typescript-eslint/no-explicit-any': 'error',
    eqeqeq: ['error', 'always']
  },
  overrides: [
    {
      files: ['*-test.js', '*-test.ts', '**/mirage/**/*.ts'],
      rules: {
        '@typescript-eslint/consistent-type-assertions': 'warn',
        '@typescript-eslint/no-non-null-assertion': 'warn',
        '@typescript-eslint/explicit-function-return-type': 'warn',
        '@typescript-eslint/no-explicit-any': 'warn'
      },
      files: ['*.js'],
      rules: {
        '@typescript-eslint/explicit-function-return-type': 'warn'
      }
    }
  ]
};
