import { SourceConfig } from '../types';
import glueLogo from '../../../../../images/gluelogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: glue
    config:
        # AWS credentials. 
        aws_region: # The region for your AWS Glue instance. 
        # Add secret in Secrets Tab with relevant names for each variable
        # The access key for your AWS account.
        aws_access_key_id: "\${AWS_ACCESS_KEY_ID}"
        # The secret key for your AWS account.
        aws_secret_access_key: "\${AWS_SECRET_KEY}"
        aws_session_token: # The session key for your AWS account. This is only needed when you are using temporary credentials.
        # aws_role: # (Optional) The role to assume (Role chaining supported by using a sorted list).

        # Allow / Deny specific databases & tables
        # database_pattern:
        #    allow:
        #        - "flights-database"
        # table_pattern:
        #    allow:
        #        - "avro"
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"
        # Add a secret in secrets Tab
        token: "\${GMS_TOKEN}"`;

const glueConfig: SourceConfig = {
    type: 'glue',
    placeholderRecipe,
    displayName: 'Glue',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/glue',
    logoUrl: glueLogo,
};

export default glueConfig;
