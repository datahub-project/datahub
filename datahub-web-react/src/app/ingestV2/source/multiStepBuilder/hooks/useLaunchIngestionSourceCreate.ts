import { useCallback } from 'react';
import { useHistory } from 'react-router';

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

    return useCallback(
        ({ sourceType, initialBuilderState, initialStepIndex = 1 }: LaunchIngestionSourceCreateParams) => {
            const locationState: IngestionSourceCreatePageLocationState = {
                initialBuilderState,
                initialStepIndex,
            };

            history.push({
                pathname: PageRoutes.INGESTION_CREATE,
                state: locationState,
                search: `?sourceType=${encodeURIComponent(sourceType)}`,
            });
        },
        [history],
    );
}
