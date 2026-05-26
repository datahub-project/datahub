import { useCallback } from 'react';
import { useHistory } from 'react-router';

import {
    GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE,
    buildGitHubDocumentsIngestionState,
} from '@app/context/import/buildGitHubDocumentsIngestionState';
import type { GitHubDocumentsIngestionLocationState } from '@app/context/import/githubDocumentsIngestion.types';
import { PageRoutes } from '@conf/Global';

type LaunchGitHubDocumentsIngestionParams = {
    parentDocumentUrn?: string | null;
};

export function useLaunchGitHubDocumentsIngestion() {
    const history = useHistory();

    return useCallback(
        ({ parentDocumentUrn }: LaunchGitHubDocumentsIngestionParams) => {
            const locationState: GitHubDocumentsIngestionLocationState = {
                initialBuilderState: buildGitHubDocumentsIngestionState({ parentDocumentUrn }),
                initialStepIndex: 1,
            };

            history.push({
                pathname: PageRoutes.INGESTION_CREATE,
                state: locationState,
                search: `?sourceType=${GITHUB_DOCUMENTS_INGESTION_SOURCE_TYPE}`,
            });
        },
        [history],
    );
}
