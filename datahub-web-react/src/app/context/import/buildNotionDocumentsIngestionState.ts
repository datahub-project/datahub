import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';

const NOTION_SOURCE_TYPE = 'notion';

type BuildNotionDocumentsIngestionStateParams = {
    parentDocumentUrn?: string | null;
};

export function buildNotionDocumentsIngestionState({ parentDocumentUrn }: BuildNotionDocumentsIngestionStateParams) {
    const parentUrnYaml = parentDocumentUrn ? `"${parentDocumentUrn}"` : 'null';

    const recipe = `source:
  type: notion
  config:
    api_key: null
    page_ids: []
    recursive: true
    document_import_mode: EXTERNAL
    parent_document_urn: ${parentUrnYaml}
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
        recipe,
    });
}

export const NOTION_INGESTION_SOURCE_TYPE = NOTION_SOURCE_TYPE;
