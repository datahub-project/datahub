/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingestV2/source/conf/types';

import mysqlLogo from '@images/mysqllogo-2.png';

const placeholderRecipe = `\
source:
    type: mysql
    config:
        # Coordinates
        host_port: # Your MySQL host and post, e.g. mysql:3306
        database: # Your MySQL database name, e.g. datahub

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${MYSQL_USERNAME}" # Your MySQL username, e.g. admin
        password: "\${MYSQL_PASSWORD}" # Your MySQL password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
`;

const mysqlConfig: SourceConfig = {
    type: 'mysql',
    placeholderRecipe,
    displayName: 'MySQL',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/mysql/',
    logoUrl: mysqlLogo,
};

export default mysqlConfig;
