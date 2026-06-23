import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';
import { CONTEXT_DOCUMENT_IMPORT_MODE } from '@app/context/import/import.types';

const CONFLUENCE_SOURCE_TYPE = 'confluence';

export function buildConfluenceDocumentsIngestionState() {
    const recipe = `source:
  type: confluence
  config:
    url: null
    username: null
    api_token: null
    cloud: true
    document_import_mode: ${CONTEXT_DOCUMENT_IMPORT_MODE}
    hierarchy:
      enabled: true
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "\${DATAHUB_GMS_URL}"
`;

    return buildIngestionSourceState({
        sourceType: CONFLUENCE_SOURCE_TYPE,
        displayName: 'Confluence',
        recipeYaml: recipe,
    });
}

export const CONFLUENCE_INGESTION_SOURCE_TYPE = CONFLUENCE_SOURCE_TYPE;
