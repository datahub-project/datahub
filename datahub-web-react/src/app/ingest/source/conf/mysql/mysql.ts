import { SourceConfig } from '../types';
import mysqlLogo from '../../../../../images/mysqllogo-2.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: mysql
    config: 
        # Coordinates
        host_port: # Your MySQL host and post, e.g. mysql:3306
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
        server: "${baseUrl}/gms"
        token: "<your-api-token-secret-here>"`;

const mysqlConfig: SourceConfig = {
    type: 'mysql',
    placeholderRecipe,
    displayName: 'MySQL',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/mysql/',
    logoUrl: mysqlLogo,
};

export default mysqlConfig;
