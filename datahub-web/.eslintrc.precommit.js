module.exports = {
  rules: {
    '@typescript-eslint/no-angle-bracket-type-assertion': 'error',
    '@typescript-eslint/no-non-null-assertion': 'error',
    '@typescript-eslint/no-object-literal-type-assertion': 'error',
    '@typescript-eslint/explicit-function-return-type': 'error',
    '@typescript-eslint/no-explicit-any': 'error'
  },
  overrides: [
    {
      files: ['*-test.js', '*-test.ts', '**/mirage/**/*.ts'],
      rules: {
        '@typescript-eslint/no-angle-bracket-type-assertion': 'warn',
        '@typescript-eslint/no-non-null-assertion': 'warn',
        '@typescript-eslint/no-object-literal-type-assertion': 'warn',
        '@typescript-eslint/explicit-function-return-type': 'warn',
        '@typescript-eslint/no-explicit-any': 'warn'
      }
    }
  ]
};
