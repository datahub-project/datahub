import { SourceConfig } from '../types';
import mysqlLogo from '../../../../../images/mysqllogo-2.png';

const placeholderRecipe = `\
source: 
    type: mysql
    config: 
        # Coordinates
        host_port: # Your MySQL host and post, e.g. mysql:3306
        database: # Your MySQL database name, e.g. datahub
    
        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${MYSQL_USERNAME}" # Your MySQL username, e.g. admin
        password: "\${MYSQL_PASSWORD}" # Your MySQL password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
`;

const mysqlConfig: SourceConfig = {
    type: 'mysql',
    placeholderRecipe,
    displayName: 'MySQL',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/mysql/',
    logoUrl: mysqlLogo,
};

export default mysqlConfig;
