import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';
import { CONTEXT_DOCUMENT_IMPORT_MODE } from '@app/context/import/import.types';

const QUIP_SOURCE_TYPE = 'quip';

export function buildQuipDocumentsIngestionState() {
    const recipe = `source:
  type: quip
  config:
    access_token: null
    base_url: "https://platform.quip.com"
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
        sourceType: QUIP_SOURCE_TYPE,
        displayName: 'Quip',
        recipeYaml: recipe,
    });
}

export const QUIP_INGESTION_SOURCE_TYPE = QUIP_SOURCE_TYPE;
