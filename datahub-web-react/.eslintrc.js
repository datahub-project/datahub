module.exports = {
    parser: '@typescript-eslint/parser', // Specifies the ESLint parser
    extends: [
        'react-app',
        'plugin:react/recommended', // Uses the recommended rules from @eslint-plugin-react
        'plugin:@typescript-eslint/recommended', // Uses the recommended rules from @typescript-eslint/eslint-plugin
        'plugin:jest/recommended',
        'airbnb-typescript',
        'airbnb/hooks',
        'prettier',
        'plugin:prettier/recommended',
    ],
    parserOptions: {
        ecmaVersion: 2020, // Allows for the parsing of modern ECMAScript features
        sourceType: 'module', // Allows for the use of imports
        ecmaFeatures: {
            jsx: true, // Allows for the parsing of JSX
        },
        project: './tsconfig.json',
    },
    rules: {
        eqeqeq: ['error', 'always'],
        'react/destructuring-assignment': 'off',
        'no-console': 'off',
        'no-debugger': 'warn',
        'require-await': 'warn',
        'import/prefer-default-export': 'off', // TODO: remove this lint rule
        'import/extensions': 'off',
        'react/jsx-props-no-spreading': 'off',
        'no-plusplus': 'off',
        'no-prototype-builtins': 'off',
        'react/require-default-props': 'off',
        'no-underscore-dangle': 'off',
        '@typescript-eslint/no-unused-vars': [
            'error',
            {
                varsIgnorePattern: '^_',
                argsIgnorePattern: '^_',
            },
        ],
        '@typescript-eslint/no-empty-interface': 'off',
        "@typescript-eslint/explicit-module-boundary-types": "off",
        "@typescript-eslint/no-explicit-any": 'off',
        "import/no-extraneous-dependencies": 'off'
    },
    settings: {
        react: {
            version: 'detect', // Tells eslint-plugin-react to automatically detect the version of React to use
        },
    },
};
