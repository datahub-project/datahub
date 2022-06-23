import { SourceConfig } from '../types';
import snowflakeLogo from '../../../../../images/snowflakelogo.png';

const placeholderRecipe = `\
source: 
    type: snowflake
    config:
        # Uncomment this section to provision the role required for ingestion
        # provision_role:
        #     enabled: true
        #     dry_run: false
        #     run_ingestion: true
        #     admin_username: "\${SNOWFLAKE_ADMIN_USER}"
        #     admin_password: "\${SNOWFLAKE_ADMIN_PASS}"

        # Your Snowflake account name
        # e.g. if URL is example48144.us-west-2.snowflakecomputing.com then use "example48144"
        account_id: "example48144"
        warehouse: # Your Snowflake warehouse name, e.g. "PROD_WH"

        # Credentials
        username: "\${SNOWFLAKE_USER}" # Create a secret SNOWFLAKE_USER in secrets Tab
        password: "\${SNOWFLAKE_PASS}" # Create a secret SNOWFLAKE_PASS in secrets Tab
        role: "datahub_role"

        # Suggest to have this set to true initially to get all lineage
        ignore_start_time_lineage: true

        # This is an alternative option to specify the start_time for lineage
        # if you don't want to look back since beginning
        # start_time: '2022-03-01T00:00:00Z'

        # Uncomment and change to only allow some database metadata to be ingested
        # database_pattern:
        #     allow:
        #     - "^ACCOUNTING_DB$"
        #     - "^MARKETING_DB$"

        # Uncomment and change to deny some metadata from few schemas 
        # schema_pattern:
        #     deny:
        #     - "information_schema.*"

        # If you want to ingest only few tables with name revenue and sales
        # table_pattern:
        #     allow:  
        #     - ".*revenue"
        #     - ".*sales"
        
`;

const snowflakeConfig: SourceConfig = {
    type: 'snowflake',
    placeholderRecipe,
    displayName: 'Snowflake',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/snowflake/',
    logoUrl: snowflakeLogo,
};

export default snowflakeConfig;
