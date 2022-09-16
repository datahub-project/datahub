import hadoopLogo from '../../../../../images/hadooplogo.png';
import { SourceConfig } from '../types';

const placeholderRecipe = `\
source: 
    type: hdfs
    config:       
        env: 'PROD'
        database: 'data_platform'
        hadoop_host: hadoopmaster.zalopay.vn
        location:
            - '/'            
        merge_schema: true
        infer_latest: false
        recursive: true
        format: 'parquet'         
        schema_pattern:
            deny:
                - '.*staging.*'
                - '.*trash.*'
        domain:
            risk:
                allow:
                    - '.*'
        
        stateful_ingestion:
            enabled: false
            remove_stale_metadata: true
datahub_api:
    server: 'http://datahub-gms:8080'
sink: 
    type: datahub-rest
    config: 
        server: "http://datahub-gms:8080"`;

const hdfsConfig: SourceConfig = {
    type: 'hadoop',
    placeholderRecipe,
    displayName: 'HDFS',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/data_lake/',
    logoUrl: hadoopLogo,
};

export default hdfsConfig;
