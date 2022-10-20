import { SourceConfig } from '../types';
import redshiftLogo from '../../../../../images/redshiftlogo.png';

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

        table_lineage_mode: stl_scan_based
        include_table_lineage: true
        include_view_lineage: true
        profiling:
            enabled: true
        stateful_ingestion:
            enabled: true
`;

export const REDSHIFT = 'redshift';

const redshiftConfig: SourceConfig = {
    type: REDSHIFT,
    placeholderRecipe,
    displayName: 'Redshift',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/redshift/',
    logoUrl: redshiftLogo,
};

export default redshiftConfig;
