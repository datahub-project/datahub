import { SourceConfig } from '@app/ingest/source/conf/types';

import glueLogo from '@images/gluelogo.png';

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
`;

const glueConfig: SourceConfig = {
    type: 'glue',
    placeholderRecipe,
    displayName: 'Glue',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/glue',
    logoUrl: glueLogo,
};

export default glueConfig;
