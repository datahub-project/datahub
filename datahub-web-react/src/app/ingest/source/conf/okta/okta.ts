import { SourceConfig } from '../types';
import oktaLogo from '../../../../../images/oktalogo.png';

const placeholderRecipe = `\
source:
    type: okta
    config:
        # Coordinates
        okta_domain: # Your Okta Domain, e.g. "dev-35531955.okta.com"

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        okta_api_token: "\${OKTA_API_TOKEN}" # Your Okta API Token, e.g. "11be4R_M2MzDqXawbTHfKGpKee0kuEOfX1RCQSRx99"

        # Optional flags to ingest users, groups, or both
        ingest_users: True
        ingest_groups: True

        # Optional: Customize the mapping to DataHub Username from an attribute appearing in the Okta User
        # profile. Reference: https://developer.okta.com/docs/reference/api/users/
        # okta_profile_to_username_attr: str = "login"
        # okta_profile_to_username_regex: str = "([^@]+)"
    
        # Optional: Customize the mapping to DataHub Group from an attribute appearing in the Okta Group
        # profile. Reference: https://developer.okta.com/docs/reference/api/groups/
        # okta_profile_to_group_name_attr: str = "name"
        # okta_profile_to_group_name_regex: str = "(.*)"
        
        # Optional: Include deprovisioned or suspended Okta users in the ingestion.
        # include_deprovisioned_users = False
        # include_suspended_users = False
`;

const oktaConfig: SourceConfig = {
    type: 'okta',
    placeholderRecipe,
    displayName: 'Okta',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/okta',
    logoUrl: oktaLogo,
};

export default oktaConfig;
