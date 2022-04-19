import { SourceConfig } from '../types';
import oracleLogo from '../../../../../images/oraclelogo.png';

const baseUrl = window.location.origin;

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
sink: 
    type: datahub-rest
    config: 
        server: "${baseUrl}/api/gms"
        # Add a secret in secrets Tab
        token: "\${GMS_TOKEN}"`;

const oracleConfig: SourceConfig = {
    type: 'oracle',
    placeholderRecipe,
    displayName: 'Oracle',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/oracle/',
    logoUrl: oracleLogo,
};

export default oracleConfig;
