import { SourceConfig } from '../types';
import azureLogo from '../../../../../images/azure-ad.png';

const placeholderRecipe = `\
source:
    type: azure-ad
    config:
        client_id: # Your Azure Client ID, e.g. "00000000-0000-0000-0000-000000000000"
        tenant_id: # Your Azure Tenant ID, e.g. "00000000-0000-0000-0000-000000000000"
        # Add secret in Secrets Tab with this name
        client_secret: "\${AZURE_AD_CLIENT_SECRET}"
        redirect: # Your Redirect URL, e.g. "https://login.microsoftonline.com/common/oauth2/nativeclient"
        authority: # Your Authority URL, e.g. "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000"
        token_url: # Your Token URL, e.g. "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token"
        graph_url: # The Graph URL, e.g. "https://graph.microsoft.com/v1.0"
        
        # Optional flags to ingest users, groups, or both
        ingest_users: True
        ingest_groups: True
        
        # Optional Allow / Deny extraction of particular Groups
        # groups_pattern:
        #    allow:
        #        - ".*"

        # Optional Allow / Deny extraction of particular Users.
        # users_pattern:
        #    allow:
        #        - ".*"
`;

const azureAdConfig: SourceConfig = {
    type: 'azure-ad',
    placeholderRecipe,
    displayName: 'Azure AD',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/azure-ad',
    logoUrl: azureLogo,
};

export default azureAdConfig;
