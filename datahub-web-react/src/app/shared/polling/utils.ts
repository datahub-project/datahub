import { useCallback, useEffect, useRef, useState } from 'react';

// NOTE: for best performance, do not change these parameters while polling is going on
type PollForDataParams = {
    maxTimesToPoll: number;
    pollIntervalMillis: number;
    // NOTE: please ensure these have been wrapped in a useCallback
    onPollTimeout: () => void;
};
export const usePollForData = ({
    maxTimesToPoll,
    pollIntervalMillis,
    onPollTimeout,
}: PollForDataParams): [(pollWithQuery: () => void) => void, () => void] => {
    const [pollWithQuery, setPollWithQuery] = useState<() => void>();

    // Polling interval tracking
    const intervalRef = useRef<NodeJS.Timeout>();
    const timesPolledRef = useRef(0);

    // Polling management event handlers
    const startPolling = (query: () => void) => {
        timesPolledRef.current = 0;
        setPollWithQuery(() => query);
    };
    const teardownInterval = useCallback(() => {
        clearInterval(intervalRef.current as any);
        intervalRef.current = undefined;
    }, []);
    const terminatePolling = useCallback(() => {
        setPollWithQuery(undefined);
        teardownInterval();
    }, [teardownInterval]);

    // Teardown polling on component teardown
    useEffect(() => {
        return teardownInterval;
    }, [teardownInterval]);

    // Hook to manage polling for result until completion
    useEffect(() => {
        // Clear interval as soon as key parameters change
        teardownInterval();
        const shouldPoll = !!pollWithQuery;
        if (shouldPoll) {
            intervalRef.current = setInterval(() => {
                pollWithQuery();
                timesPolledRef.current += 1;
                const isTimeout = timesPolledRef.current === maxTimesToPoll;
                if (isTimeout) {
                    onPollTimeout();
                }
            }, pollIntervalMillis);
        }
    }, [
        pollWithQuery,
        // These should never change while polling is going on, just including bc typescript wants it
        maxTimesToPoll,
        pollIntervalMillis,
        onPollTimeout,
        teardownInterval,
    ]);

    return [startPolling, terminatePolling];
};
