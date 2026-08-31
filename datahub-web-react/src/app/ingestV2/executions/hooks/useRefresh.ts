import { useMemo } from 'react';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';
import { TabType } from '@app/ingestV2/types';

import { ExecutionRequest } from '@types';

export default function useRefresh(
    executionRequests: ExecutionRequest[],
    refresh: () => void,
    isLoading: boolean,
    selectedTab: TabType | null | undefined,
) {
    const shouldRun = useMemo(
        () => executionRequests.some(isExecutionRequestActive) && selectedTab === TabType.RunHistory,
        [executionRequests, selectedTab],
    );

    useRefreshInterval(refresh, isLoading, shouldRun);
}
