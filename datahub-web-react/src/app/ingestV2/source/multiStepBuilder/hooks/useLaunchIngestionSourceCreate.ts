import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router';

import type { IngestionSourceCreatePageLocationState } from '@app/ingestV2/source/multiStepBuilder/ingestionCreatePage.types';
import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { PageRoutes } from '@conf/Global';

export type LaunchIngestionSourceCreateParams = {
    sourceType: string;
    initialBuilderState: MultiStepSourceBuilderState;
    initialStepIndex?: number;
};

export function useLaunchIngestionSourceCreate() {
    const history = useHistory();
    const location = useLocation();

    return useCallback(
        ({ sourceType, initialBuilderState, initialStepIndex = 1 }: LaunchIngestionSourceCreateParams) => {
            const locationState: IngestionSourceCreatePageLocationState = {
                initialBuilderState,
                initialStepIndex,
                backUrl: `${location.pathname}${location.search}`,
            };

            history.push(`${PageRoutes.INGESTION_CREATE}?sourceType=${encodeURIComponent(sourceType)}`, locationState);
        },
        [history, location.pathname, location.search],
    );
}
