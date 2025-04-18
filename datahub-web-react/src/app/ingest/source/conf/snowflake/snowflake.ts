import { SourceConfig } from '@app/ingest/source/conf/types';

import snowflakeLogo from '@images/snowflakelogo.png';

const placeholderRecipe = `\
source:
    type: snowflake
    config:
        account_id: abcde
        username: "\${SNOWFLAKE_DATAHUB_USER}"
        password: "\${SNOWFLAKE_DATAHUB_PASSWORD}"
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
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/snowflake/',
    logoUrl: snowflakeLogo,
};

export default snowflakeConfig;
