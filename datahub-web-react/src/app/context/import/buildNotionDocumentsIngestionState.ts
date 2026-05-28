import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';
import { CONTEXT_DOCUMENT_IMPORT_MODE } from '@app/context/import/import.types';

const NOTION_SOURCE_TYPE = 'notion';

export function buildNotionDocumentsIngestionState() {
    const recipe = `source:
  type: notion
  config:
    api_key: null
    page_ids: []
    recursive: true
    document_import_mode: ${CONTEXT_DOCUMENT_IMPORT_MODE}
    hierarchy:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "\${DATAHUB_GMS_URL}"
`;

    return buildIngestionSourceState({
        sourceType: NOTION_SOURCE_TYPE,
        displayName: 'Notion',
        recipeYaml: recipe,
    });
}

export const NOTION_INGESTION_SOURCE_TYPE = NOTION_SOURCE_TYPE;
