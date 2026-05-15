/** @type {import('eslint').Linter.Config} */
module.exports = {
    root: true,
    parser: '@typescript-eslint/parser',
    parserOptions: {
        ecmaVersion: 2022,
        sourceType: 'module',
        project: './tsconfig.json',
    },
    plugins: ['@typescript-eslint', 'playwright'],
    extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended', 'plugin:playwright/recommended'],
    rules: {
        // Enforce no floating promises — critical in async test code
        '@typescript-eslint/no-floating-promises': 'error',

        // Allow explicit `any` sparingly (test helpers sometimes need it)
        '@typescript-eslint/no-explicit-any': 'warn',

        // _ prefix is the TS convention for intentionally-unused parameters
        '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],

        // networkidle, force:true, waitForTimeout, and expect-expect are intentionally used
        // across DataHub Playwright tests for reasons documented at the usage sites:
        //   - networkidle: GraphQL responses remain in-flight after navigation
        //   - force/waitForTimeout: Ant Design component workarounds
        //   - expect-expect: assertions delegated into POM helper methods
        'playwright/no-networkidle': 'off',
        'playwright/no-force-option': 'off',
        'playwright/no-wait-for-timeout': 'off',
        'playwright/expect-expect': 'off',
        'playwright/no-wait-for-selector': 'off',
        'playwright/no-skipped-test': 'off',
        'playwright/no-conditional-in-test': 'off',

        'no-console': 'error',
    },
    env: {
        node: true,
        es2022: true,
    },
    ignorePatterns: ['node_modules/', 'playwright-report/', 'test-results/'],
};