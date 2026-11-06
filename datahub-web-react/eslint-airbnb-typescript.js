/* eslint-disable import/no-extraneous-dependencies */
// Adapted from eslint-config-airbnb-typescript 17.0.0 (MIT, Copyright © Matt Turnbull).
// That package only supports @typescript-eslint v5-v7, so this keeps its TypeScript
// equivalents of Airbnb's rules for @typescript-eslint v8. Formatting rules are left out:
// they were removed in v8, and `prettier` turns them off anyway.
const { rules: baseBestPracticesRules } = require('eslint-config-airbnb-base/rules/best-practices');
const { rules: baseES6Rules } = require('eslint-config-airbnb-base/rules/es6');
const { rules: baseImportsRules } = require('eslint-config-airbnb-base/rules/imports');
const { rules: baseStyleRules } = require('eslint-config-airbnb-base/rules/style');
const { rules: baseVariablesRules } = require('eslint-config-airbnb-base/rules/variables');

module.exports = {
    plugins: ['@typescript-eslint'],
    parser: '@typescript-eslint/parser',
    settings: {
        'import/parsers': {
            '@typescript-eslint/parser': ['.ts', '.tsx', '.d.ts'],
        },
        'import/resolver': {
            node: {
                extensions: ['.mjs', '.js', '.jsx', '.json', '.ts', '.tsx', '.d.ts'],
            },
        },
        'import/extensions': ['.js', '.mjs', '.jsx', '.ts', '.tsx', '.d.ts'],
        'import/external-module-folders': ['node_modules', 'node_modules/@types'],
    },
    rules: {
        camelcase: 'off',
        '@typescript-eslint/naming-convention': [
            'error',
            { selector: 'variable', format: ['camelCase', 'PascalCase', 'UPPER_CASE'] },
            { selector: 'function', format: ['camelCase', 'PascalCase'] },
            { selector: 'typeLike', format: ['PascalCase'] },
        ],
        'default-param-last': 'off',
        '@typescript-eslint/default-param-last': baseBestPracticesRules['default-param-last'],
        'dot-notation': 'off',
        '@typescript-eslint/dot-notation': baseBestPracticesRules['dot-notation'],
        'no-array-constructor': 'off',
        '@typescript-eslint/no-array-constructor': baseStyleRules['no-array-constructor'],
        'no-dupe-class-members': 'off',
        '@typescript-eslint/no-dupe-class-members': baseES6Rules['no-dupe-class-members'],
        'no-empty-function': 'off',
        '@typescript-eslint/no-empty-function': baseBestPracticesRules['no-empty-function'],
        'no-implied-eval': 'off',
        'no-new-func': 'off',
        '@typescript-eslint/no-implied-eval': baseBestPracticesRules['no-implied-eval'],
        'no-loop-func': 'off',
        '@typescript-eslint/no-loop-func': baseBestPracticesRules['no-loop-func'],
        'no-redeclare': 'off',
        '@typescript-eslint/no-redeclare': baseBestPracticesRules['no-redeclare'],
        'no-shadow': 'off',
        '@typescript-eslint/no-shadow': baseVariablesRules['no-shadow'],
        'no-throw-literal': 'off',
        '@typescript-eslint/only-throw-error': baseBestPracticesRules['no-throw-literal'],
        'no-unused-expressions': 'off',
        '@typescript-eslint/no-unused-expressions': baseBestPracticesRules['no-unused-expressions'],
        'no-unused-vars': 'off',
        '@typescript-eslint/no-unused-vars': baseVariablesRules['no-unused-vars'],
        'no-use-before-define': 'off',
        '@typescript-eslint/no-use-before-define': baseVariablesRules['no-use-before-define'],
        'no-useless-constructor': 'off',
        '@typescript-eslint/no-useless-constructor': baseES6Rules['no-useless-constructor'],
        'require-await': 'off',
        '@typescript-eslint/require-await': baseBestPracticesRules['require-await'],
        'no-return-await': 'off',
        '@typescript-eslint/return-await': [baseBestPracticesRules['no-return-await'], 'in-try-catch'],
        'import/extensions': [
            baseImportsRules['import/extensions'][0],
            baseImportsRules['import/extensions'][1],
            { ...baseImportsRules['import/extensions'][2], ts: 'never', tsx: 'never' },
        ],
        'import/no-extraneous-dependencies': [
            baseImportsRules['import/no-extraneous-dependencies'][0],
            {
                ...baseImportsRules['import/no-extraneous-dependencies'][1],
                devDependencies: baseImportsRules['import/no-extraneous-dependencies'][1].devDependencies.flatMap(
                    (devDep) => {
                        const devDepWithTs = devDep.replace(/\bjs(x?)\b/g, 'ts$1');
                        return devDepWithTs === devDep ? [devDep] : [devDep, devDepWithTs];
                    },
                ),
            },
        ],
        'react/jsx-filename-extension': ['error', { extensions: ['.jsx', '.tsx'] }],
    },
    overrides: [
        {
            // The TypeScript compiler already checks these, and some report false positives on TS syntax
            files: ['*.ts', '*.tsx'],
            rules: {
                'constructor-super': 'off',
                'getter-return': 'off',
                'no-const-assign': 'off',
                'no-dupe-args': 'off',
                'no-dupe-class-members': 'off',
                'no-dupe-keys': 'off',
                'no-func-assign': 'off',
                'no-import-assign': 'off',
                'no-new-symbol': 'off',
                'no-obj-calls': 'off',
                'no-redeclare': 'off',
                'no-setter-return': 'off',
                'no-this-before-super': 'off',
                'no-undef': 'off',
                'no-unreachable': 'off',
                'no-unsafe-negation': 'off',
                'valid-typeof': 'off',
                'import/named': 'off',
                'import/no-named-as-default-member': 'off',
                'import/no-unresolved': 'off',
            },
        },
    ],
};
