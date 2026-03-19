import { SourceConfig } from '@app/ingestV2/source/conf/types';

import hightouchLogo from '@images/hightouchlogo.png';

const placeholderRecipe = `\
source:
    type: hightouch
    config:
        api_config:
            api_key: "\${HIGHTOUCH_API_KEY}"
        env: "PROD"
        emit_models_as_datasets: true
        include_column_lineage: true
        include_sync_runs: true
        stateful_ingestion:
            enabled: true
`;

export const HIGHTOUCH = 'hightouch';

const hightouchConfig: SourceConfig = {
    type: HIGHTOUCH,
    placeholderRecipe,
    displayName: 'Hightouch',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/hightouch/',
    logoUrl: hightouchLogo,
};

export default hightouchConfig;
