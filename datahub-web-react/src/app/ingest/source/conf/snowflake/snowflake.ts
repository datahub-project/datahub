import { SourceConfig } from '@app/ingest/source/conf/types';

import snowflakeLogo from '@images/snowflakelogo.png';

const placeholderRecipe = `\
source:
    type: snowflake
    config:
        account_id: abcde
        username: "\${SNOWFLAKE_DATAHUB_USER}"
        # For password authentication:
        password: "\${SNOWFLAKE_DATAHUB_PASSWORD}"
        # For private key authentication:
        # authentication_type: KEY_PAIR_AUTHENTICATOR
        # private_key: "\${SNOWFLAKE_PRIVATE_KEY}"
        # private_key_password: "\${SNOWFLAKE_PRIVATE_KEY_PASSWORD}"  # Optional if key is encrypted
        warehouse: "\${DATAHUB_WAREHOUSE}"
        role: datahub_role
        profiling:
            turn_off_expensive_profiling_metrics: true
            enabled: true
        stateful_ingestion:
            enabled: true
`;

export const SNOWFLAKE = 'snowflake';

const snowflakeConfig: SourceConfig = {
    type: SNOWFLAKE,
    placeholderRecipe,
    displayName: 'Snowflake',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/snowflake/',
    logoUrl: snowflakeLogo,
};

export default snowflakeConfig;
