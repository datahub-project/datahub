/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingestV2/source/conf/types';

import lookerLogo from '@images/lookerlogo.svg';

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
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/looker/',
    logoUrl: lookerLogo,
};

export default lookerConfig;
