import { useEffect, useMemo, useRef } from 'react';

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
    const previousTabRef = useRef(selectedTab);

    const shouldRun = useMemo(
        () => executionRequests.some(isExecutionRequestActive) && selectedTab === TabType.RunHistory,
        [executionRequests, selectedTab],
    );

    // Trigger immediate refresh when switching to Run History tab
    useEffect(() => {
        const isRunHistoryTab = selectedTab === TabType.RunHistory;
        const wasPreviouslyRunHistoryTab = previousTabRef.current === TabType.RunHistory;
        const hasActiveExecutions = executionRequests.some(isExecutionRequestActive);

        // If we switched to Run History tab and have active executions, refresh immediately
        if (isRunHistoryTab && !wasPreviouslyRunHistoryTab && hasActiveExecutions && !isLoading) {
            refresh();
        }

        previousTabRef.current = selectedTab;
    }, [selectedTab, executionRequests, refresh, isLoading]);

    useRefreshInterval(refresh, isLoading, shouldRun);
}
