import { SourceConfig } from '../types';
import verticaLogo from '../../../../../images/vertica_logo.png';

const placeholderRecipe = `\
source:
  type: vertica
  config:

    #Coordinates
    host_port: # Your Postgres host and port, e.g. vertica_ce:5433
    database: # Your Postgres Database, e.g. VMArt
    #Credentials
    username: "username" # Your Vertica username, e.g. dbadmin
    password: "pass"     # Your Vertica password, e.g. password


    include_tables: true
    include_views: true
    include_projections: true
    include_models: true
    include_oauth: true
    include_view_lineage: true
    include_projection_lineage: true

      
    profiling: 
      enabled: true
      
    stateful_ingestion:
        enabled: true    
`;

export const VERTICA = 'vertica';

const verticaConfig: SourceConfig = {
    type: VERTICA,
    placeholderRecipe,
    displayName: 'Vertica',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/vertica/',
    logoUrl: verticaLogo,
};

export default verticaConfig;
