import { SourceConfig } from '../types';
import lookerLogo from '../../../../../images/lookerlogo.svg';

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
`;

export const LOOKER = 'looker';

const lookerConfig: SourceConfig = {
    type: LOOKER,
    placeholderRecipe,
    displayName: 'Looker',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/looker/',
    logoUrl: lookerLogo,
};

export default lookerConfig;
