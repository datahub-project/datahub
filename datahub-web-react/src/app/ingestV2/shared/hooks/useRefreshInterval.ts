import { useEffect, useState } from 'react';

const REFRESH_INTERVAL_MS = 3000;

export default function useRefreshInterval(refresh: () => void, shouldRefreshFn: () => boolean) {
    const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);

    useEffect(() => {
        const shouldRefresh = shouldRefreshFn();
        if (shouldRefresh) {
            if (!refreshInterval) {
                const interval = setInterval(refresh, REFRESH_INTERVAL_MS);
                setRefreshInterval(interval);
            }
        } else if (refreshInterval) {
            clearInterval(refreshInterval);
            setRefreshInterval(null);
        }
    }, [refreshInterval, refresh, shouldRefreshFn]);
}
