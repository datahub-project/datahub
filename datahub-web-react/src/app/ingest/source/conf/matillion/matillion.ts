import { SourceConfig } from '@app/ingest/source/conf/types';

import matillionLogo from '@images/matillionlogo.png';

const placeholderRecipe = `\
source:
    type: matillion
    config:
        api_config:
            api_token: "\${MATILLION_API_TOKEN}"
            base_url: "https://eu1.api.matillion.com/dpc"
        env: "PROD"
        include_pipeline_executions: true
        extract_projects_to_containers: true
        stateful_ingestion:
            enabled: true
`;

export const MATILLION = 'matillion';

const matillionConfig: SourceConfig = {
    type: MATILLION,
    placeholderRecipe,
    displayName: 'Matillion',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/matillion/',
    logoUrl: matillionLogo,
};

export default matillionConfig;
