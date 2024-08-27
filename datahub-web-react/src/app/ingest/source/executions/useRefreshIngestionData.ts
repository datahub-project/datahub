import { useState, useEffect } from 'react';

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
