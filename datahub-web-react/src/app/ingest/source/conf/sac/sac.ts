import { SourceConfig } from '../types';
import sacLogo from '../../../../../images/saclogo.svg';

const placeholderRecipe = `\
source:
    type: sac
    config:
        tenant_url: # Your SAP Analytics Cloud tenant URL, e.g. https://company.eu10.sapanalytics.cloud or https://company.eu10.hcs.cloud.sap
        token_url: # The Token URL of your SAP Analytics Cloud tenant, e.g. https://company.eu10.hana.ondemand.com/oauth/token.

        # Add secret in Secrets Tab with relevant names for each variable
        client_id: "\${SAC_CLIENT_ID}" # Your SAP Analytics Cloud client id
        client_secret: "\${SAC_CLIENT_SECRET}" # Your SAP Analytics Cloud client secret
`;

export const SAC = 'sac';

const sacConfig: SourceConfig = {
    type: SAC,
    placeholderRecipe,
    displayName: 'SAP Analytics Cloud',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/sac/',
    logoUrl: sacLogo,
};

export default sacConfig;
