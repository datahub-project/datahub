module.exports = {
    semi: true,
    trailingComma: 'all',
    singleQuote: true,
    printWidth: 120,
    tabWidth: 4,
    plugins: ['@trivago/prettier-plugin-sort-imports'],
    importOrder: [
        // Third party imports are first automatically, in their own group
        '^@components/(.*)$',
        // Have to specify all aliases otherwise they're considered third party
        '^(@app|@conf|@providers|@utils|@src)/(.*)$',
        '^(@graphql/|@graphql-mock/|@types)(.*)$',
        '^@images/(.*)$',
        '^[./]',
    ],
    importOrderSeparation: true,
    importOrderSortSpecifiers: true,
};
