import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import type {
    IngestionSourceCreatePageLocationState,
    IngestionSourceListDeepLinkState,
} from '@app/ingestV2/source/multiStepBuilder/ingestionCreatePage.types';
import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { PageRoutes } from '@conf/Global';

export type LaunchIngestionSourceCreateParams = {
    sourceType: string;
    initialBuilderState: MultiStepSourceBuilderState;
    initialStepIndex?: number;
};

export function useLaunchIngestionSourceCreate() {
    const history = useHistory();
    const showIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    return useCallback(
        ({ sourceType, initialBuilderState, initialStepIndex = 1 }: LaunchIngestionSourceCreateParams) => {
            if (showIngestionOnboardingRedesignV1) {
                const locationState: IngestionSourceCreatePageLocationState = {
                    initialBuilderState,
                    initialStepIndex,
                };

                history.push({
                    pathname: PageRoutes.INGESTION_CREATE,
                    state: locationState,
                    search: `?sourceType=${encodeURIComponent(sourceType)}`,
                });
                return;
            }

            const locationState: IngestionSourceListDeepLinkState = {
                openCreateIngestionModal: true,
                initialBuilderState,
                sourceType,
            };

            history.push({
                pathname: tabUrlMap[TabType.Sources],
                state: locationState,
            });
        },
        [history, showIngestionOnboardingRedesignV1],
    );
}
