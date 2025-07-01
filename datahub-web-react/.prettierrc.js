module.exports = {
    semi: true,
    trailingComma: 'all',
    singleQuote: true,
    printWidth: 120,
    tabWidth: 4,
    plugins: ['@trivago/prettier-plugin-sort-imports'],
    importOrder: [
        '.*\.less$',
        '<THIRD_PARTY_MODULES>',
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
