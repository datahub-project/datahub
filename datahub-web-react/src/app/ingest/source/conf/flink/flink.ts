import { SourceConfig } from '@app/ingest/source/conf/types';

import flinkLogo from '@images/flinklogo.svg';

const placeholderRecipe = `\
source:
    type: flink
    config:
        connection:
            rest_api_url: "http://localhost:8081"
            # sql_gateway_url: "http://localhost:8083"  # Optional: for catalog metadata
        # include_lineage: true
        # include_run_history: true
        stateful_ingestion:
            enabled: true

`;

export const FLINK = 'flink';

const flinkConfig: SourceConfig = {
    type: FLINK,
    placeholderRecipe,
    displayName: 'Flink',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/flink/',
    logoUrl: flinkLogo,
};

export default flinkConfig;
