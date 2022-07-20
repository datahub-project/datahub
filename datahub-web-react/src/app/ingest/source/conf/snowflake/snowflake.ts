import { SourceConfig } from '../types';
import snowflakeLogo from '../../../../../images/snowflakelogo.png';

const placeholderRecipe = `\
source: 
    type: snowflake
    config:
        account_id: "example_id"
        warehouse: "example_warehouse"
        role: "datahub_role"
        ignore_start_time_lineage: true
        include_table_lineage: true
        include_view_lineage: true
        check_role_grants: true
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
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/snowflake/',
    logoUrl: snowflakeLogo,
};

export default snowflakeConfig;
