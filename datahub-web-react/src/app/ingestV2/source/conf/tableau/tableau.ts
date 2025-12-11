/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingestV2/source/conf/types';

import tableauLogo from '@images/tableaulogo.png';

const placeholderRecipe = `\
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com
    site: acryl
    projects: ["default", "Project 2"]

    # Credentials
    username: "\${TABLEAU_USER}"
    password: "\${TABLEAU_PASSWORD}"

    # Options
    ingest_tags: True
    ingest_owner: True
    default_schema_map:
      mydatabase: public
      anotherdatabase: anotherschema
`;

export const TABLEAU = 'tableau';

const tableauConfig: SourceConfig = {
    type: TABLEAU,
    placeholderRecipe,
    displayName: 'Tableau',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/tableau/',
    logoUrl: tableauLogo,
};

export default tableauConfig;
