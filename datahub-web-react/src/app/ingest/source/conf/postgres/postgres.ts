import { SourceConfig } from '../types';
import postgresLogo from '../../../../../images/postgreslogo.png';

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
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/postgres/',
    logoUrl: postgresLogo,
};

export default postgresConfig;
