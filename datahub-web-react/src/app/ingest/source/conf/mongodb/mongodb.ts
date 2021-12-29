import { SourceConfig } from '../types';
import mongodbLogo from '../../../../../images/mongodblogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: mongodb
    config:
        # Coordinates
        connect_uri: # Your MongoDB connect URI, e.g. "mongodb://localhost"

        # Credentials
        username: # Your MongoDB username, e.g. admin
        password: # Your MongoDB password, e.g. password_01

        # Options (recommended)
        enableSchemaInference: True
        useRandomSampling: True
        maxSchemaSize: 300
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"`;

const mongoConfig: SourceConfig = {
    type: 'mongodb',
    placeholderRecipe,
    displayName: 'MongoDB',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/mongodb/',
    logoUrl: mongodbLogo,
};

export default mongoConfig;
