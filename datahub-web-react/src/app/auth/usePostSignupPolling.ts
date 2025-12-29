import { useEffect } from 'react';

import { USER_SIGNED_UP_KEY } from '@app/auth/constants';

const POLL_INTERVAL_MS = 5000; // Poll every 5 seconds
const MAX_POLL_DURATION_MS = 60000; // Stop polling after 60 seconds

type UsePostSignupPollingOptions = {
    onPoll: () => void;
    pollIntervalMs?: number;
    maxPollDurationMs?: number;
};

/**
 * Hook that polls for updated user permissions after signup.
 *
 * When a user signs up with an invite token, their role permissions may take
 * a few seconds to propagate due to backend caching. This hook detects when
 * a user has just signed up (via localStorage flag) and periodically refetches
 * their user data until the max duration is reached.
 *
 * @param options.onPoll - Callback to execute on each poll (typically refetches user data)
 * @param options.pollIntervalMs - How often to poll in milliseconds (default: 7000)
 * @param options.maxPollDurationMs - Maximum time to poll in milliseconds (default: 60000)
 */
export function usePostSignupPolling({
    onPoll,
    pollIntervalMs = POLL_INTERVAL_MS,
    maxPollDurationMs = MAX_POLL_DURATION_MS,
}: UsePostSignupPollingOptions) {
    useEffect(() => {
        const signupTimestamp = localStorage.getItem(USER_SIGNED_UP_KEY);
        if (!signupTimestamp) return undefined;

        const signupTime = parseInt(signupTimestamp, 10);
        const elapsed = Date.now() - signupTime;

        // Stop polling if max duration already exceeded
        if (elapsed > maxPollDurationMs) {
            localStorage.removeItem(USER_SIGNED_UP_KEY);
            return undefined;
        }

        // Start polling interval
        const intervalId = setInterval(() => {
            const currentElapsed = Date.now() - signupTime;
            if (currentElapsed > maxPollDurationMs) {
                clearInterval(intervalId);
                localStorage.removeItem(USER_SIGNED_UP_KEY);
                return;
            }

            onPoll();
        }, pollIntervalMs);

        return () => clearInterval(intervalId);
    }, [onPoll, pollIntervalMs, maxPollDurationMs]);
}
