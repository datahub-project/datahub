import { useEffect } from 'react';

const REFRESH_INTERVAL_MS = 3000;

export default function useRefreshInterval(refresh: () => void, isLoading: boolean, shouldRefresh: boolean) {
    useEffect(() => {
        if (isLoading) return () => {};

        if (!shouldRefresh) return () => {};

        const timer = setTimeout(() => refresh(), REFRESH_INTERVAL_MS);

        return () => {
            clearTimeout(timer);
        };
    }, [refresh, isLoading, shouldRefresh]);
}
