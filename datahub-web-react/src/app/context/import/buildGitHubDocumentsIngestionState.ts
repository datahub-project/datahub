import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';

const GITHUB_DOCUMENTS_SOURCE_TYPE = 'github-documents';

type BuildGitHubDocumentsIngestionStateParams = {
    parentDocumentUrn?: string | null;
};

export function buildGitHubDocumentsIngestionState({ parentDocumentUrn }: BuildGitHubDocumentsIngestionStateParams) {
    const parentUrnYaml = parentDocumentUrn ? `"${parentDocumentUrn}"` : 'null';

    const recipe = `source:
  type: github-documents
  config:
    github_token: null
    repository: ""
    branch: main
    path_prefix: ""
    file_extensions:
      - .md
      - .txt
    parent_document_urn: ${parentUrnYaml}
    document_import_mode: NATIVE
    show_in_global_context: true

sink:
  type: datahub-rest
  config:
    server: "\${DATAHUB_GMS_URL}"
`;

    return buildIngestionSourceState({
        sourceType: GITHUB_DOCUMENTS_SOURCE_TYPE,
        displayName: 'GitHub Documents',
        recipe,
    });
}

export const GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE = GITHUB_DOCUMENTS_SOURCE_TYPE;
