import { SourceConfig } from '@app/ingest/source/conf/types';

import hbaseLogo from '@images/hbaselogo.png';

const placeholderRecipe = `\
source: 
    type: hbase
    config:
        # Coordinates
        host: # Your HBase Thrift server host, e.g. localhost
        port: 9090 # Your HBase Thrift server port (default: 9090)
        
        # Optional: Filter patterns
        namespace_pattern:
            allow:
                - ".*"  # Allow all namespaces
        table_pattern:
            allow:
                - ".*"  # Allow all tables
        
        # Optional: Authentication
        # auth_mechanism: # Authentication mechanism (e.g., KERBEROS)
        
        # Optional: Schema extraction
        include_column_families: true # Include column families in schema metadata
        max_column_qualifiers: 100 # Maximum column qualifiers to sample
        
        stateful_ingestion:
            enabled: true
`;

export const HBASE = 'hbase';

const hbaseConfig: SourceConfig = {
    type: HBASE,
    placeholderRecipe,
    displayName: 'HBase',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/hbase/',
    logoUrl: hbaseLogo,
};

export default hbaseConfig;
