import { SourceConfig } from '../types';
import mongodbLogo from '../../../../../images/mongodblogo.png';

const placeholderRecipe = `\
source:
    type: mongodb
    config:
        # Coordinates
        connect_uri: # Your MongoDB connect URI, e.g. "mongodb://localhost"

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${MONGO_USERNAME}" # Your MongoDB username, e.g. admin
        password: "\${MONGO_PASSWORD}" # Your MongoDB password, e.g. password_01

        # Options (recommended)
        enableSchemaInference: True
        useRandomSampling: True
        maxSchemaSize: 300
`;

const mongoConfig: SourceConfig = {
    type: 'mongodb',
    placeholderRecipe,
    displayName: 'MongoDB',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/mongodb/',
    logoUrl: mongodbLogo,
};

export default mongoConfig;
