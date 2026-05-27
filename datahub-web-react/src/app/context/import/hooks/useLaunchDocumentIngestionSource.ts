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
import { ImportSourceType } from '@app/context/import/import.types';
import { useLaunchIngestionSourceCreate } from '@app/ingestV2/source/multiStepBuilder/hooks/useLaunchIngestionSourceCreate';

type LaunchDocumentIngestionSourceParams = {
    source: ImportSourceType;
    parentDocumentUrn?: string | null;
};

export function useLaunchDocumentIngestionSource() {
    const launchIngestionSourceCreate = useLaunchIngestionSourceCreate();

    return useCallback(
        ({ source, parentDocumentUrn }: LaunchDocumentIngestionSourceParams) => {
            switch (source) {
                case ImportSourceType.GITHUB:
                    launchIngestionSourceCreate({
                        sourceType: GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildGitHubDocumentsIngestionState({ parentDocumentUrn }),
                    });
                    break;
                case ImportSourceType.NOTION:
                    launchIngestionSourceCreate({
                        sourceType: NOTION_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildNotionDocumentsIngestionState({ parentDocumentUrn }),
                    });
                    break;
                case ImportSourceType.CONFLUENCE:
                    launchIngestionSourceCreate({
                        sourceType: CONFLUENCE_INGESTION_SOURCE_TYPE,
                        initialBuilderState: buildConfluenceDocumentsIngestionState({ parentDocumentUrn }),
                    });
                    break;
                default:
                    break;
            }
        },
        [launchIngestionSourceCreate],
    );
}
