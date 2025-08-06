import { SourceConfig } from '@app/ingest/source/conf/types';

import snowflakeLogo from '@images/snowflakelogo.png';

const placeholderRecipe = `\
source: 
    type: snowflake
    config:
        account_id: "example_id"
        warehouse: "example_warehouse"
        role: "datahub_role"
        include_table_lineage: true
        include_view_lineage: true
        profiling:
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
