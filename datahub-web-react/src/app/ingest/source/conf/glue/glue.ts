import { SourceConfig } from '../types';
import glueLogo from '../../../../../images/gluelogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: glue
    config:
        # AWS credentials. 
        aws_region: # The region for your AWS Glue instance. 
        aws_access_key_id: # The access key for your AWS account.
        aws_secret_access_key: # The secret key for your AWS account.
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
<<<<<<< HEAD
        server: "${baseUrl}/gms"
        token: "<your-api-token-secret-here>"`;
=======
        server: "${baseUrl}/api/gms"`;
>>>>>>> master

const glueConfig: SourceConfig = {
    type: 'glue',
    placeholderRecipe,
    displayName: 'Glue',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/glue',
    logoUrl: glueLogo,
};

export default glueConfig;
