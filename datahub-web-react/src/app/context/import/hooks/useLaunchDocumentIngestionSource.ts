import { useCallback } from 'react';

import {
    CONFLUENCE_INGESTION_SOURCE_TYPE,
    buildConfluenceDocumentsIngestionState,
} from '@app/context/import/buildConfluenceDocumentsIngestionState';
import {
    GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE,
    buildGitHubDocumentsIngestionState,
} from '@app/context/import/buildGitHubDocumentsIngestionState';
import {
    NOTION_INGESTION_SOURCE_TYPE,
    buildNotionDocumentsIngestionState,
} from '@app/context/import/buildNotionDocumentsIngestionState';
import {
    QUIP_INGESTION_SOURCE_TYPE,
    buildQuipDocumentsIngestionState,
} from '@app/context/import/buildQuipDocumentsIngestionState';
import { ImportSourceType } from '@app/context/import/import.types';
import { useLaunchIngestionSourceCreate } from '@app/ingestV2/source/multiStepBuilder/hooks/useLaunchIngestionSourceCreate';

type LaunchDocumentIngestionSourceParams = {
    source: ImportSourceType;
};

export function useLaunchDocumentIngestionSource() {
    const launchIngestionSourceCreate = useLaunchIngestionSourceCreate();

    return useCallback(
        ({ source }: LaunchDocumentIngestionSourceParams) => {
            switch (source) {
                case ImportSourceType.GITHUB:
                    launchIngestionSourceCreate({
                        sourceType: GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildGitHubDocumentsIngestionState(),
                    });
                    break;
                case ImportSourceType.NOTION:
                    launchIngestionSourceCreate({
                        sourceType: NOTION_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildNotionDocumentsIngestionState(),
                    });
                    break;
                case ImportSourceType.CONFLUENCE:
                    launchIngestionSourceCreate({
                        sourceType: CONFLUENCE_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildConfluenceDocumentsIngestionState(),
                    });
                    break;
                case ImportSourceType.QUIP:
                    launchIngestionSourceCreate({
                        sourceType: QUIP_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildQuipDocumentsIngestionState(),
                    });
                    break;
                default:
                    break;
            }
        },
        [launchIngestionSourceCreate],
    );
}
