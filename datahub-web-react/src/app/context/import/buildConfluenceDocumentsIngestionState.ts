import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';

const CONFLUENCE_SOURCE_TYPE = 'confluence';

type BuildConfluenceDocumentsIngestionStateParams = {
    parentDocumentUrn?: string | null;
};

export function buildConfluenceDocumentsIngestionState({
    parentDocumentUrn,
}: BuildConfluenceDocumentsIngestionStateParams) {
    const parentUrnYaml = parentDocumentUrn ? `"${parentDocumentUrn}"` : 'null';

    const recipe = `source:
  type: confluence
  config:
    url: null
    username: null
    api_token: null
    cloud: true
    document_import_mode: NATIVE
    parent_document_urn: ${parentUrnYaml}
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
        recipe,
    });
}

export const CONFLUENCE_INGESTION_SOURCE_TYPE = CONFLUENCE_SOURCE_TYPE;
