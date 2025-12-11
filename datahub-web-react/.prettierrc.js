/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
