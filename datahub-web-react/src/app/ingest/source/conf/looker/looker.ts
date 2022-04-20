import { SourceConfig } from '../types';
import lookerLogo from '../../../../../images/lookerlogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: looker
    config:
        # Coordinates
        base_url: # Your Looker instance URL, e.g. https://company.looker.com:19999

        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        client_id: "\${LOOKER_CLIENT_ID}" # Your Looker client id, e.g. admin
        client_secret: "\${LOOKER_CLIENT_SECRET}" # Your Looker password, e.g. password_01
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"
        # Add a secret in secrets Tab
        token: "\${GMS_TOKEN}"`;

const lookerConfig: SourceConfig = {
    type: 'looker',
    placeholderRecipe,
    displayName: 'Looker',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/looker/',
    logoUrl: lookerLogo,
};

export default lookerConfig;
