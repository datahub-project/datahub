const { execSync } = require('child_process');
const path = require('path');

// --------------------------------------------------------------------------
// Semantic color token enforcement (dark mode initiative)
//
// These rules block hardcoded colors and deprecated color imports, but ONLY
// in files changed on the current branch. This lets us gradually migrate
// without breaking the entire codebase.
//
// Once migration is complete, move these rules into the top-level `rules`
// block and remove the git-diff scoping.
// --------------------------------------------------------------------------
const rulesDirPlugin = require('eslint-plugin-rulesdir');
rulesDirPlugin.RULES_DIR = path.join(__dirname, 'eslint-rules');

const repoRoot = path.resolve(__dirname, '..');
let changedTsFiles = [];
try {
    let baseBranch;
    try {
        execSync('git rev-parse --verify origin/master', { encoding: 'utf-8', cwd: repoRoot, stdio: 'pipe' });
        baseBranch = 'origin/master';
    } catch {
        baseBranch = 'origin/main';
    }

    const raw = execSync(
        `git diff --diff-filter=d --name-only ${baseBranch} -- "datahub-web-react/src/**/*.ts" "datahub-web-react/src/**/*.tsx"`,
        { encoding: 'utf-8', cwd: repoRoot, stdio: 'pipe' },
    );
    changedTsFiles = raw
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((f) => f.replace(/^datahub-web-react\//, ''));
} catch {
    // If git is unavailable (e.g. shallow clone in CI), skip scoped rules
}

const COLOR_ENFORCEMENT_RULES = {
    'no-restricted-imports': [
        'error',
        {
            patterns: [
                {
                    group: ['@conf/theme/colorThemes/color', '**/conf/theme/colorThemes/color'],
                    message:
                        'Do not import the raw color palette. Use semantic tokens via `props.theme.colors.*` or `useTheme().colors.*`. See colorThemes/types.ts.',
                },
                {
                    group: [
                        '@components/theme/foundations/colors',
                        '**/alchemy-components/theme/foundations/colors',
                    ],
                    message:
                        'Do not import alchemy colors directly. Use semantic tokens via `props.theme.colors.*` or `useTheme().colors.*`. See colorThemes/types.ts.',
                },
            ],
            paths: [
                {
                    name: '@app/entity/shared/constants',
                    importNames: ['ANTD_GRAY', 'ANTD_GRAY_V2', 'REDESIGN_COLORS'],
                    message:
                        'ANTD_GRAY / REDESIGN_COLORS are deprecated. Use semantic tokens via `props.theme.colors.*` or `useTheme().colors.*`.',
                },
                {
                    name: '@app/entityV2/shared/constants',
                    importNames: ['ANTD_GRAY', 'ANTD_GRAY_V2', 'REDESIGN_COLORS', 'LINEAGE_COLORS'],
                    message:
                        'ANTD_GRAY / REDESIGN_COLORS / LINEAGE_COLORS are deprecated. Use semantic tokens via `props.theme.colors.*` or `useTheme().colors.*`.',
                },
            ],
        },
    ],
    'rulesdir/no-hardcoded-colors': 'error',
};

// Files that legitimately need raw color values
const COLOR_RULE_EXCLUDED_FILES = [
    'src/conf/theme/colorThemes/**',
    'src/conf/theme/*.ts',
    'src/conf/theme/*.json',
    'src/alchemy-components/theme/**',
];

module.exports = {
    parser: '@typescript-eslint/parser',
    extends: [
        'airbnb',
        'airbnb-typescript',
        'airbnb/hooks',
        'plugin:@typescript-eslint/recommended',
        'plugin:vitest/recommended',
        'prettier',
    ],
    plugins: ['@typescript-eslint', '@stylistic/js', 'react-refresh', 'import-alias', 'rulesdir'],
    parserOptions: {
        ecmaVersion: 2020,
        sourceType: 'module',
        ecmaFeatures: {
            jsx: true,
        },
        project: './tsconfig.json',
        tsconfigRootDir: __dirname,
    },
    rules: {
        '@typescript-eslint/no-explicit-any': 'off',
        '@stylistic/js/comma-dangle': ['error', 'always-multiline'],
        'arrow-body-style': 'off',
        'class-methods-use-this': 'off',
        'import/no-extraneous-dependencies': 'off',
        'import/no-relative-packages': 'error',
        'import/prefer-default-export': 'off', // TODO: remove this lint rule
        'no-console': 'off',
        'no-plusplus': 'off',
        'no-prototype-builtins': 'off',
        'no-restricted-exports': ['off', { restrictedNamedExports: ['default', 'then'] }],
        'no-underscore-dangle': 'off',
        'no-unsafe-optional-chaining': 'off',
        'prefer-exponentiation-operator': 'off',
        'prefer-regex-literals': 'off',
        'react/destructuring-assignment': 'off',
        'react/function-component-definition': 'off',
        'react/jsx-no-bind': 'off',
        'react/jsx-no-constructed-context-values': 'off',
        'react/jsx-no-useless-fragment': 'off',
        'react/jsx-props-no-spreading': 'off',
        'react/no-unstable-nested-components': 'off',
        'react/require-default-props': 'off',
        '@typescript-eslint/no-unused-vars': [
            'error',
            {
                varsIgnorePattern: '^_',
                argsIgnorePattern: '^_',
            },
        ],
        'vitest/prefer-to-be': 'off',
        '@typescript-eslint/no-use-before-define': ['error', { functions: false, classes: false }],
        'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
        'import-alias/import-alias': [
            'error',
            {
                aliases: [
                    // Must be kept consistent with tsconfig.json, vite.config.ts, and .prettierrc.js
                    { alias: '@components/', matcher: '^src/alchemy-components/' },
                    { alias: '@app/', matcher: '^src/app/' },
                    { alias: '@conf/', matcher: '^src/conf/' },
                    { alias: '@graphql/', matcher: '^src/graphql/' },
                    { alias: '@graphql-mock/', matcher: '^src/graphql-mock/' },
                    { alias: '@images/', matcher: '^src/images/' },
                    { alias: '@providers/', matcher: '^src/providers/' },
                    { alias: '@utils/', matcher: '^src/utils/' },
                    { alias: '@types', matcher: '^src/types.generated' },
                    { alias: '@src/', matcher: '^src/' },
                ],
            },
        ],
    },
    settings: {
        react: {
            version: 'detect',
        },
    },
    overrides: [
        {
            files: ['src/app/searchV2/**/*.tsx', 'src/app/entityV2/**/*.tsx'],
            rules: { 'import/no-cycle': 'off' },
        },
        {
            files: ['src/alchemy-components/theme/**/*.ts'],
            rules: { 'import/no-relative-packages': 'off', 'import-alias/import-alias': 'off' },
        },
        // Semantic color enforcement — only on files changed in the current branch
        ...(changedTsFiles.length > 0
            ? [
                  {
                      files: changedTsFiles,
                      excludedFiles: COLOR_RULE_EXCLUDED_FILES,
                      rules: COLOR_ENFORCEMENT_RULES,
                  },
              ]
            : []),
    ],
};
