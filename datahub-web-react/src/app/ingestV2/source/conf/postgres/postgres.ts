/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingestV2/source/conf/types';

import postgresLogo from '@images/postgreslogo.png';

const placeholderRecipe = `\
source:
    type: postgres
    config:
        # Coordinates
        host_port: # Your Postgres host and port, e.g. postgres:5432
        database: # Your Postgres Database, e.g. sample_db

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${POSTGRES_USERNAME}" # Your Postgres username, e.g. admin
        password: "\${POSTGRES_PASSWORD}" # Your Postgres password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
        stateful_ingestion:
            enabled: true
`;

export const POSTGRES = 'postgres';

const postgresConfig: SourceConfig = {
    type: POSTGRES,
    placeholderRecipe,
    displayName: 'Postgres',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/postgres/',
    logoUrl: postgresLogo,
};

export default postgresConfig;
