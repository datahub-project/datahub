import { SourceConfig } from '../types';
import arangoDBLogo from '../../../../../images/arangodblogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
  type: arangodb
  config:
    # Coordinates
    host_port: # Your ArangoDB host and port, e.g. http://arangodb:8529
    database: # Your ArangoDB database name, e.g. user_network (Optional, if not specified, ingests from all databases)

    # Credentials
    username: # Your ArangoDB username, e.g. admin
    password: # Your ArangoDB password, e.g. password_01

sink:
  type: datahub-rest
  config:
    server: "${baseUrl}/api/gms"`;

const arangoDBConfig: SourceConfig = {
    type: 'arangodb',
    placeholderRecipe,
    displayName: 'ArangoDB',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/mongodb',
    logoUrl: arangoDBLogo,
};
export default arangoDBConfig;
