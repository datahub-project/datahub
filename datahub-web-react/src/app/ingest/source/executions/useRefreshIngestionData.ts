/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

export default function useRefreshIngestionData(refresh: () => void, hasActiveExecution: () => boolean) {
    const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);

    useEffect(() => {
        const shouldRefresh = hasActiveExecution();
        if (shouldRefresh) {
            if (!refreshInterval) {
                const interval = setInterval(refresh, 3000);
                setRefreshInterval(interval);
            }
        } else if (refreshInterval) {
            clearInterval(refreshInterval);
            setRefreshInterval(null);
        }
    }, [refreshInterval, refresh, hasActiveExecution]);
}
