/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingest/source/conf/types';

import hiveLogo from '@images/hivelogo.png';

const placeholderRecipe = `\
source: 
    type: hive
    config:
        # Coordinates
        host_port: # Your Hive host and port, e.g. hive:10000
        database: # Your Hive database name, e.g. SampleDatabase (Optional, if not specified, ingests from all databases)

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${HIVE_USERNAME}" # Your Hive username, e.g. admin
        password: "\${HIVE_PASSWORD}"# Your Hive password, e.g. password_01
        stateful_ingestion:
            enabled: true
`;

export const HIVE = 'hive';

const hiveConfig: SourceConfig = {
    type: HIVE,
    placeholderRecipe,
    displayName: 'Hive',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/hive/',
    logoUrl: hiveLogo,
};

export default hiveConfig;
