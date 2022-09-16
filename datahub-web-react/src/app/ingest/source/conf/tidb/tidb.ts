import { SourceConfig } from '../types';
import tidbLogo from '../../../../../images/tidblogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: tidb
    config: 
        # Coordinates
        host_port: # Your MySQL host and post, e.g. tidb:3306
        database: # Your MySQL database name, e.g. datahub
    
        # Credentials
        username: # Your MySQL username, e.g. admin
        password: # Your MySQL password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"`;

const mysqlConfig: SourceConfig = {
    type: 'tidb',
    placeholderRecipe,
    displayName: 'TiDB',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/mysql/',
    logoUrl: tidbLogo,
};

export default mysqlConfig;
