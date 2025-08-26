import { SourceConfig } from '@app/ingest/source/conf/types';

import databricksLogo from '@images/databrickslogo.png';

const placeholderRecipe = `\
source:
    type: unity-catalog
    config:
        # Coordinates
        workspace_url: null
        warehouse_id: null
        token: null
        include_table_lineage: true
        include_column_lineage: false
        stateful_ingestion:
            enabled: true  
`;

export const UNITY_CATALOG = 'unity-catalog';

const databricksConfig: SourceConfig = {
    type: UNITY_CATALOG,
    placeholderRecipe,
    displayName: 'Databricks',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/databricks/',
    logoUrl: databricksLogo,
};

export default databricksConfig;
