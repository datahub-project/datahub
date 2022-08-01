import { SourceConfig } from '../types';
import bigqueryLogo from '../../../../../images/bigquerylogo.png';

const placeholderRecipe = `\
source:
    type: bigquery
    config:
        # Coordinates
        project_id: # Your BigQuery project id, e.g. sample_project_id
        # Credentials
        credential:
            project_id: # Your BQ project id, e.g. sample_project_id
            private_key_id: "\${BQ_PRIVATE_KEY_ID}"
            private_key: "\${BQ_PRIVATE_KEY}"
            client_email: # Your BQ client email, e.g. "test@suppproject-id-1234567.iam.gserviceaccount.com"
            client_id: # Your BQ client id, e.g. "123456678890"
            
            include_table_lineage: true
            include_view_lineage: true
            profiling:
                enabled: true
            stateful_ingestion:
                enabled: true
`;

export const BIGQUERY = 'bigquery';

const bigqueryConfig: SourceConfig = {
    type: BIGQUERY,
    placeholderRecipe,
    displayName: 'BigQuery',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/bigquery/',
    logoUrl: bigqueryLogo,
};

export default bigqueryConfig;
