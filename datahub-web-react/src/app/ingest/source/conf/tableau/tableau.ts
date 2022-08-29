import { SourceConfig } from '../types';
import tableauLogo from '../../../../../images/tableaulogo.png';

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
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/tableau/',
    logoUrl: tableauLogo,
};

export default tableauConfig;
