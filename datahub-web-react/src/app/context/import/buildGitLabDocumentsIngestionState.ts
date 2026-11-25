import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';
import { CONTEXT_DOCUMENT_IMPORT_MODE } from '@app/context/import/import.types';

const GITLAB_DOCUMENTS_SOURCE_TYPE = 'gitlab-documents';

export function buildGitLabDocumentsIngestionState() {
    const recipe = `source:
  type: gitlab-documents
  config:
    gitlab_token: null
    base_url: https://gitlab.com
    repository: ""
    branch: main
    path_prefix: ""
    file_extensions:
      - .md
      - .txt
    document_import_mode: ${CONTEXT_DOCUMENT_IMPORT_MODE}
    create_repo_root_document: true
    show_in_global_context: true
    stateful_ingestion:
      enabled: true
`;

    return buildIngestionSourceState({
        sourceType: GITLAB_DOCUMENTS_SOURCE_TYPE,
        displayName: 'GitLab',
        recipeYaml: recipe,
    });
}

export const GITLAB_DOCUMENTS_INGESTION_SOURCE_TYPE = GITLAB_DOCUMENTS_SOURCE_TYPE;
