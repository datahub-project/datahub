module.exports = {
    extends: '../.eslintrc.js',
    parserOptions: {
        project: 'cypress/tsconfig.json',
    },
    globals: {
        Cypress: true,
    },
    rules: {
        'jest/expect-expect': 0,
    },
};
