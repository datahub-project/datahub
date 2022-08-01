import { SourceConfig } from '../types';
import hiveLogo from '../../../../../images/hivelogo.png';

const placeholderRecipe = `\
source: 
    type: hive
    config:
        # Coordinates
        host_port: # Your Hive host and port, e.g. hive:10000
        database: # Your Hive database name, e.g. SampleDatabase (Optional, if not specified, ingests from all databases)

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${HIVE_USERNAME}" # Your Hive username, e.g. admin
        password: "\${HIVE_PASSWORD}"# Your Hive password, e.g. password_01
`;

const hiveConfig: SourceConfig = {
    type: 'hive',
    placeholderRecipe,
    displayName: 'Hive',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/hive/',
    logoUrl: hiveLogo,
};

export default hiveConfig;
