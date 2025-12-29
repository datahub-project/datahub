import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { USER_SIGNED_UP_KEY } from '@app/auth/constants';
import { usePostSignupPolling } from '@app/auth/usePostSignupPolling';

describe('usePostSignupPolling', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        localStorage.clear();
    });

    afterEach(() => {
        vi.restoreAllMocks();
        localStorage.clear();
    });

    it('should not poll when signup flag is not present', () => {
        const onPoll = vi.fn();

        renderHook(() => usePostSignupPolling({ onPoll }));

        vi.advanceTimersByTime(10000);

        expect(onPoll).not.toHaveBeenCalled();
    });

    it('should poll at specified interval when signup flag is present', () => {
        const onPoll = vi.fn();
        localStorage.setItem(USER_SIGNED_UP_KEY, Date.now().toString());

        renderHook(() => usePostSignupPolling({ onPoll, pollIntervalMs: 5000 }));

        // Should not call immediately
        expect(onPoll).not.toHaveBeenCalled();

        // After first interval
        vi.advanceTimersByTime(5000);
        expect(onPoll).toHaveBeenCalledTimes(1);

        // After second interval
        vi.advanceTimersByTime(5000);
        expect(onPoll).toHaveBeenCalledTimes(2);
    });

    it('should stop polling after max duration', () => {
        const onPoll = vi.fn();
        localStorage.setItem(USER_SIGNED_UP_KEY, Date.now().toString());

        renderHook(() =>
            usePostSignupPolling({
                onPoll,
                pollIntervalMs: 5000,
                maxPollDurationMs: 15000,
            }),
        );

        // Poll at 5s, 10s
        vi.advanceTimersByTime(10000);
        expect(onPoll).toHaveBeenCalledTimes(2);

        // At 15s, should stop polling
        vi.advanceTimersByTime(5000);
        expect(onPoll).toHaveBeenCalledTimes(3);

        // After 20s, no more polls
        vi.advanceTimersByTime(5000);
        expect(onPoll).toHaveBeenCalledTimes(3);
    });

    it('should remove localStorage flag after max duration', () => {
        const onPoll = vi.fn();
        localStorage.setItem(USER_SIGNED_UP_KEY, Date.now().toString());

        renderHook(() =>
            usePostSignupPolling({
                onPoll,
                pollIntervalMs: 5000,
                maxPollDurationMs: 10000,
            }),
        );

        expect(localStorage.getItem(USER_SIGNED_UP_KEY)).toBeTruthy();

        // Advance past max duration
        vi.advanceTimersByTime(15000);

        expect(localStorage.getItem(USER_SIGNED_UP_KEY)).toBeNull();
    });

    it('should not start polling if signup time has already exceeded max duration', () => {
        const onPoll = vi.fn();
        const oldTimestamp = Date.now() - 70000; // 70 seconds ago
        localStorage.setItem(USER_SIGNED_UP_KEY, oldTimestamp.toString());

        renderHook(() =>
            usePostSignupPolling({
                onPoll,
                maxPollDurationMs: 60000,
            }),
        );

        vi.advanceTimersByTime(10000);

        expect(onPoll).not.toHaveBeenCalled();
        expect(localStorage.getItem(USER_SIGNED_UP_KEY)).toBeNull();
    });

    it('should cleanup interval on unmount', () => {
        const onPoll = vi.fn();
        localStorage.setItem(USER_SIGNED_UP_KEY, Date.now().toString());

        const { unmount } = renderHook(() => usePostSignupPolling({ onPoll, pollIntervalMs: 5000 }));

        vi.advanceTimersByTime(5000);
        expect(onPoll).toHaveBeenCalledTimes(1);

        unmount();

        // After unmount, no more polls
        vi.advanceTimersByTime(10000);
        expect(onPoll).toHaveBeenCalledTimes(1);
    });
});
