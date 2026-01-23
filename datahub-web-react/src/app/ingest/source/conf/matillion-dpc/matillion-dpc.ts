import { SourceConfig } from '@app/ingest/source/conf/types';

import matillionLogo from '@images/matillionlogo.png';

const placeholderRecipe = `\
source:
    type: matillion-dpc
    config:
        api_config:
            api_token: "\${MATILLION_API_TOKEN}"
            region: US1  # or EU1
        env: "PROD"
        include_pipeline_executions: true
        include_lineage: true
        include_column_lineage: true
        parse_sql_for_lineage: false
        extract_projects_to_containers: true
        stateful_ingestion:
            enabled: true
`;

export const MATILLION_DPC = 'matillion-dpc';

const matillionConfig: SourceConfig = {
    type: MATILLION_DPC,
    placeholderRecipe,
    displayName: 'Matillion',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/matillion-dpc/',
    logoUrl: matillionLogo,
};

export default matillionConfig;
