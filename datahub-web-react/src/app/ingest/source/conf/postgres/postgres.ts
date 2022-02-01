import { SourceConfig } from '../types';
import postgresLogo from '../../../../../images/postgreslogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: postgres
    config:
        # Coordinates
        host_port: # Your Postgres host and port, e.g. postgres:5432
        database: # Your Postgres Database, e.g. sample_db

        # Credentials
        username: # Your Postgres username, e.g. admin
        password: # Your Postgres password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
sink: 
    type: datahub-rest 
    config: 
<<<<<<< HEAD
        server: "${baseUrl}/gms"
        token: "<your-api-token-secret-here>"`;
=======
        server: "${baseUrl}/api/gms"`;
>>>>>>> master

const postgresConfig: SourceConfig = {
    type: 'postgres',
    placeholderRecipe,
    displayName: 'Postgres',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/postgres/',
    logoUrl: postgresLogo,
};

export default postgresConfig;
