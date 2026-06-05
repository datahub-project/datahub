const tseslint = require('typescript-eslint');
const playwright = require('eslint-plugin-playwright');
const globals = require('globals');

module.exports = tseslint.config(
    { ignores: ['eslint.config.js', 'node_modules/**', 'playwright-report/**', 'test-results/**'] },

    tseslint.configs.recommended,

    playwright.configs['flat/recommended'],

    {
        files: ['**/*.ts'],
        languageOptions: {
            globals: globals.node,
            ecmaVersion: 2026,
            sourceType: 'module',
            parserOptions: { project: './tsconfig.json' },
        },
        rules: {
            // Enforce no floating promises — critical in async test code
            '@typescript-eslint/no-floating-promises': 'error',

            // Allow explicit `any` sparingly (test helpers sometimes need it)
            '@typescript-eslint/no-explicit-any': 'warn',

            // _ prefix is the TS convention for intentionally-unused parameters
            '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],

            // --- Locator discipline -------------------------------------------------
            // Raw CSS/XPath strings must live in page objects, never in spec files.
            // Exemptions: iframe selectors have no ARIA equivalent.
            'playwright/no-raw-locators': ['error', { allowed: ['iframe'] }],

            // Prefer semantic built-ins (getByRole, getByLabel, …) over page.locator()
            'playwright/prefer-native-locators': 'warn',

            // first()/nth() usually signals a missing data-testid
            'playwright/no-nth-methods': 'warn',

            // --- Deprecated / fragile APIs -----------------------------------------
            // ElementHandle is deprecated; use Locator API instead
            'playwright/no-element-handle': 'error',

            // page.$eval / page.$$eval are hard to debug and race-prone
            'playwright/no-eval': 'error',

            // --- Correctness -------------------------------------------------------
            // Catches forgotten await on Playwright calls
            'playwright/missing-playwright-await': 'error',

            // Prevents assertions outside test blocks
            'playwright/no-standalone-expect': 'error',

            // Blocks accidental .only from being committed
            'playwright/no-focused-test': 'error',

            // expect(locator).toBeVisible() > expect(await locator.isVisible()).toBe(true)
            'playwright/prefer-web-first-assertions': 'error',

            // --- Code quality ------------------------------------------------------
            'playwright/no-duplicate-hooks': 'error',
            'playwright/prefer-hooks-on-top': 'warn',

            // --- Intentional overrides (documented) --------------------------------
            // networkidle: GraphQL responses remain in-flight after navigation
            'playwright/no-networkidle': 'off',
            // force/waitForTimeout: Ant Design component workarounds
            'playwright/no-force-option': 'off',
            'playwright/no-wait-for-timeout': 'off',
            // expect-expect: assertions delegated into POM helper methods
            'playwright/expect-expect': 'off',
            'playwright/no-wait-for-selector': 'off',
            'playwright/no-skipped-test': 'off',
            'playwright/no-conditional-in-test': 'off',

            'no-console': 'error',
        },
    },
);
