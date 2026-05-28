import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';
import { CONTEXT_DOCUMENT_IMPORT_MODE } from '@app/context/import/import.types';

const GITHUB_DOCUMENTS_SOURCE_TYPE = 'github-documents';

export function buildGitHubDocumentsIngestionState() {
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
    document_import_mode: ${CONTEXT_DOCUMENT_IMPORT_MODE}
    show_in_global_context: true

sink:
  type: datahub-rest
  config:
    server: "\${DATAHUB_GMS_URL}"
`;

    return buildIngestionSourceState({
        sourceType: GITHUB_DOCUMENTS_SOURCE_TYPE,
        displayName: 'GitHub',
        recipeYaml: recipe,
    });
}

export const GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE = GITHUB_DOCUMENTS_SOURCE_TYPE;
