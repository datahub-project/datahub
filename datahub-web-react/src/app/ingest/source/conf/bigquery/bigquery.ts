import { SourceConfig } from '../types';
import bigqueryLogo from '../../../../../images/bigquerylogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: bigquery
    config:
        # Coordinates
        project_id: # Your BigQuery project id, e.g. sample_project_id
        # Credentials
        credential:
            project_id: # Your BQ project id, e.g. sample_project_id
            private_key_id: # Your BQ private key id, e.g. "d0121d0000882411234e11166c6aaa23ed5d74e0"
            private_key: # Your BQ private key, e.g. "-----BEGIN PRIVATE KEY-----\\nMIIyourkey\\n-----END PRIVATE KEY-----\\n"
            client_email: # Your BQ client email, e.g. "test@suppproject-id-1234567.iam.gserviceaccount.com"
            client_id: # Your BQ client id, e.g. "123456678890"
sink: 
    type: datahub-rest
    config: 
<<<<<<< HEAD
        server: "${baseUrl}/gms"
        token: "<your-api-token-secret-here>"`;
=======
        server: "${baseUrl}/api/gms"`;
>>>>>>> master

const bigqueryConfig: SourceConfig = {
    type: 'bigquery',
    placeholderRecipe,
    displayName: 'BigQuery',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/bigquery/',
    logoUrl: bigqueryLogo,
};

export default bigqueryConfig;
