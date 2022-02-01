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
        username: # Your Redshift username, e.g. admin
        password: # Your Redshift password, e.g. password_01

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

const redshiftConfig: SourceConfig = {
    type: 'redshift',
    placeholderRecipe,
    displayName: 'Redshift',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/redshift/',
    logoUrl: redshiftLogo,
};

export default redshiftConfig;
