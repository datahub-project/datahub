import { ApolloQueryResult } from '@apollo/client';
import { message } from 'antd';
import { useCallback, useEffect, useRef, useState } from 'react';

import { GetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';

const POLLING_TIMEOUT_MS = 15000; // 15 seconds

/**
 * Hook to poll for new prediction generation after monitor settings update.
 * Compares generatedAt timestamps to determine when new predictions are available.
 */
export const usePollForNewPredictions = (
    refetchMonitor: () => Promise<ApolloQueryResult<GetAssertionWithMonitorsQuery>>,
    currentGeneratedAt: string | number | null | undefined,
    intervalMs = 3000,
) => {
    const [isPolling, setIsPolling] = useState(false);
    const [initialGeneratedAt, setInitialGeneratedAt] = useState<string | number | null | undefined>(null);
    const onPollingCompleteRef = useRef<(() => void) | null>(null);

    // Start polling and capture the current generatedAt as baseline
    const startPolling = useCallback(
        (onPollingComplete?: () => void) => {
            setIsPolling(true);
            setInitialGeneratedAt(currentGeneratedAt);
            onPollingCompleteRef.current = onPollingComplete || null;
        },
        [currentGeneratedAt],
    );

    // Stop polling and reset state
    const stopPolling = useCallback(() => {
        setIsPolling(false);
        setInitialGeneratedAt(null);
        // Invoke callback when polling completes
        const callback = onPollingCompleteRef.current;
        onPollingCompleteRef.current = null;
        if (callback) {
            callback();
        }
    }, []);

    // Polling interval effect - refetch monitor data periodically
    useEffect(() => {
        if (!isPolling) return undefined;

        const interval = setInterval(async () => {
            try {
                await refetchMonitor();
            } catch (error) {
                console.error('Error during polling refetch:', error);
                stopPolling();
            }
        }, intervalMs);

        return () => clearInterval(interval);
    }, [isPolling, refetchMonitor, intervalMs, stopPolling]);

    // Auto-stop polling when new predictions are detected (generatedAt changed)
    useEffect(() => {
        if (isPolling && initialGeneratedAt !== null && currentGeneratedAt !== initialGeneratedAt) {
            stopPolling();
        }
    }, [isPolling, currentGeneratedAt, initialGeneratedAt, stopPolling]);

    // Timeout effect - stop polling after 15 seconds
    useEffect(() => {
        if (!isPolling) return undefined;

        const timeout = setTimeout(() => {
            console.warn(`Polling for new predictions timed out after ${POLLING_TIMEOUT_MS / 1000} seconds`);
            message.warning(`Polling for new predictions timed out after ${POLLING_TIMEOUT_MS / 1000} seconds`);

            stopPolling();
        }, POLLING_TIMEOUT_MS);

        return () => clearTimeout(timeout);
    }, [isPolling, stopPolling]);

    return { isPolling, startPolling, stopPolling };
};
