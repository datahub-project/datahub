import { SourceConfig } from '../types';
import redshiftLogo from '../../../../../images/redshiftlogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: redshift
    config:
        # Coordinates
        host_port: # Your Redshift host and post, e.g. example.something.us-west-2.redshift.amazonaws.com:5439
        database: # Your Redshift database, e.g. SampleDatabase

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${REDSHIFT_USERNAME}" # Your Redshift username, e.g. admin
        password: "\${REDSHIFT_PASSWORD}" # Your Redshift password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"
        # Add a secret in secrets Tab
        token: "\${GMS_TOKEN}"`;

const redshiftConfig: SourceConfig = {
    type: 'redshift',
    placeholderRecipe,
    displayName: 'Redshift',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/redshift/',
    logoUrl: redshiftLogo,
};

export default redshiftConfig;
