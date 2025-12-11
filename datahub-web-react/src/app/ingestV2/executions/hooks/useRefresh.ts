/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
