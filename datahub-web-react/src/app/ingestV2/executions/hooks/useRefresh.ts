import { useCallback } from 'react';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';

import { ExecutionRequest } from '@types';

export default function useRefresh(executionRequests: ExecutionRequest[], refresh: () => void) {
    const hasRunningExecutions = useCallback(
        () => executionRequests.some(isExecutionRequestActive),
        [executionRequests],
    );

    useRefreshInterval(refresh, hasRunningExecutions);
}
