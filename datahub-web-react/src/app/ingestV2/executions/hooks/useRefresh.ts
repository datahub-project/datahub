import { useMemo } from 'react';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import useRefreshInterval from '@app/ingestV2/shared/hooks/useRefreshInterval';

import { ExecutionRequest } from '@types';

export default function useRefresh(
    executionRequests: ExecutionRequest[],
    refresh: () => void,
    isLoading: boolean,
    isActive: boolean,
) {
    const shouldRun = useMemo(
        () => executionRequests.some(isExecutionRequestActive) && isActive,
        [executionRequests, isActive],
    );

    useRefreshInterval(refresh, isLoading, shouldRun);
}
