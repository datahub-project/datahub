import { SourceConfig } from '../types';
import snowflakeLogo from '../../../../../images/snowflakelogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: snowflake
    config:
        # Coordinates
        host_port: # Your Snowflake host and port, e.g. my-snowflake-account.us-west-2.snowflakecomputing.com
        warehouse: # Your Snowflake warehouse name, e.g. PROD_WH

        # Credentials
        username: # Your Snowflake username, e.g. admin
        password: # Your Snowflake password, e.g. password_01
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"`;

const snowflakeConfig: SourceConfig = {
    type: 'snowflake',
    placeholderRecipe,
    displayName: 'Snowflake',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/snowflake/',
    logoUrl: snowflakeLogo,
};

export default snowflakeConfig;
