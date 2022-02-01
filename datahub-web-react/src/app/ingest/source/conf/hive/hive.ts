import { SourceConfig } from '../types';
import hiveLogo from '../../../../../images/hivelogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source: 
    type: hive
    config:
        # Coordinates
        host_port: # Your Hive host and port, e.g. hive:10000
        database: # Your Hive database name, e.g. SampleDatabase (Optional, if not specified, ingests from all databases)

        # Credentials
        username: # Your Hive username, e.g. admin
        password: # Your Hive password, e.g. password_01

sink: 
    type: datahub-rest
    config: 
        server: "${baseUrl}/gms"
        token: "<your-api-token-secret-here>"`;

const hiveConfig: SourceConfig = {
    type: 'hive',
    placeholderRecipe,
    displayName: 'Hive',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/hive/',
    logoUrl: hiveLogo,
};

export default hiveConfig;
