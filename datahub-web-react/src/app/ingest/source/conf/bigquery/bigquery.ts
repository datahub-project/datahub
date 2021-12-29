import { SourceConfig } from '../types';
import bigqueryLogo from '../../../../../images/bigquerylogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: bigquery
    config:
        # Coordinates
        project_id: # Your BigQuery project id, e.g. sample_project_id
sink: 
    type: datahub-rest
    config: 
        server: "${baseUrl}/api/gms"`;

const bigqueryConfig: SourceConfig = {
    type: 'bigquery',
    placeholderRecipe,
    displayName: 'BigQuery',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/bigquery/',
    logoUrl: bigqueryLogo,
};

export default bigqueryConfig;
