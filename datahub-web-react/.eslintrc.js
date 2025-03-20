module.exports = {
    parser: '@typescript-eslint/parser', // Specifies the ESLint parser
    extends: [
        'airbnb',
        'airbnb-typescript',
        'airbnb/hooks',
        'plugin:@typescript-eslint/recommended',
        'plugin:vitest/recommended',
        'prettier',
    ],
    plugins: ['@typescript-eslint', 'react-refresh'],
    parserOptions: {
        ecmaVersion: 2020, // Allows for the parsing of modern ECMAScript features
        sourceType: 'module', // Allows for the use of imports
        ecmaFeatures: {
            jsx: true, // Allows for the parsing of JSX
        },
        project: './tsconfig.json',
    },
    rules: {
        '@typescript-eslint/no-explicit-any': 'off',
        'arrow-body-style': 'off',
        'class-methods-use-this': 'off',
        'import/no-extraneous-dependencies': 'off',
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
    },
    settings: {
        react: {
            version: 'detect', // Tells eslint-plugin-react to automatically detect the version of React to use
        },
    },
	overrides: [
		{
			files: ['src/app/searchV2/**/*.tsx', 'src/app/entityV2/**/*.tsx'],
			rules: {
				'import/no-cycle': 'off',
			},
		},
	],
};
