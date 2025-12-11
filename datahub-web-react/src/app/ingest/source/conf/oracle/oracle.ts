/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingest/source/conf/types';

import oracleLogo from '@images/oraclelogo.png';

const placeholderRecipe = `\
source: 
    type: oracle
    config:
        # Coordinates
        host_port: # Your Oracle host and port, e.g. oracle:5432
        database: # Your Oracle database name, e.g. sample_db

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${ORACLE_USERNAME}" # Your Oracle username, e.g. admin
        password: "\${ORACLE_PASSWORD}" # Your Oracle password, e.g. password_01

        # Optional service name
        # service_name: # Your service name, e.g. svc # omit database if using this option
`;

const oracleConfig: SourceConfig = {
    type: 'oracle',
    placeholderRecipe,
    displayName: 'Oracle',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/oracle/',
    logoUrl: oracleLogo,
};

export default oracleConfig;
