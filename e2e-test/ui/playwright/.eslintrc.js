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

        // networkidle is used intentionally across DataHub navigations because GraphQL responses
        // remain in-flight after the URL changes. Warn rather than error to allow justification
        // comments to be added case-by-case per the waitFor comment policy.
        'playwright/no-networkidle': 'warn',

        // Tests that delegate assertions into POM helpers (expect* methods) will not have
        // top-level expect() calls. Once the assertion refactor (step 3-4) is complete this
        // can be tightened. For now, recognise the POM method naming convention.
        'playwright/expect-expect': [
            'warn',
            {
                assertFunctionNames: ['expect', 'expect*', '**.expect*'],
            },
        ],


        // force:true and waitForTimeout are used intentionally for Ant Design / react-slick
        // workarounds. Each usage must have a comment (enforced by code review).
        'playwright/no-force-option': 'warn',
        'playwright/no-wait-for-timeout': 'warn',

        // console is acceptable in utility/cleanup code but not in tests
        'no-console': 'warn',
    },
    env: {
        node: true,
        es2022: true,
    },
    ignorePatterns: ['node_modules/', 'playwright-report/', 'test-results/'],
};