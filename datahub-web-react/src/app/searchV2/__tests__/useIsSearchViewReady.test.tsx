import { act, renderHook } from '@testing-library/react-hooks';
import React, { useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext, UserContextType } from '@app/context/userContext';
import {
    SEARCH_VIEW_READY_TIMEOUT_MS,
    isSearchViewReady,
    useIsSearchViewReady,
} from '@app/searchV2/useIsSearchViewReady';

const STORED_VIEW_URN = 'urn:li:dataHubView:default';

function renderViewReady(hasSetDefaultView: boolean, selectedViewUrn: string | null | undefined) {
    const value: UserContextType = {
        loaded: true,
        state: {
            ...DEFAULT_STATE,
            views: {
                ...DEFAULT_STATE.views,
                hasSetDefaultView,
            },
        },
        localState: { selectedViewUrn },
        updateLocalState: () => null,
        updateState: () => null,
        refetchUser: () => null,
    };
    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <UserContext.Provider value={value}>{children}</UserContext.Provider>
    );
    return renderHook(() => useIsSearchViewReady(), { wrapper });
}

describe('isSearchViewReady', () => {
    it('is false when the default view is unresolved and nothing is stored', () => {
        expect(isSearchViewReady(false, undefined)).toBe(false);
    });

    it('is true once resolution finishes with no view', () => {
        expect(isSearchViewReady(true, undefined)).toBe(true);
    });

    it('is true when a view URN is already stored, before resolution finishes', () => {
        expect(isSearchViewReady(false, STORED_VIEW_URN)).toBe(true);
    });

    it('is true when the stored view was explicitly cleared', () => {
        expect(isSearchViewReady(false, null)).toBe(true);
    });

    it('is true when resolution finished and a view is selected', () => {
        expect(isSearchViewReady(true, STORED_VIEW_URN)).toBe(true);
    });
});

describe('useIsSearchViewReady', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('stays false until the timeout when resolution never finishes', () => {
        expect(SEARCH_VIEW_READY_TIMEOUT_MS).toBe(10_000);
        const { result } = renderViewReady(false, undefined);

        expect(result.current).toBe(false);

        act(() => {
            vi.advanceTimersByTime(SEARCH_VIEW_READY_TIMEOUT_MS - 1);
        });
        expect(result.current).toBe(false);

        act(() => {
            vi.advanceTimersByTime(1);
        });
        expect(result.current).toBe(true);
    });

    it('is true immediately when a view is already stored', () => {
        const { result } = renderViewReady(false, STORED_VIEW_URN);
        expect(result.current).toBe(true);
    });

    it('stays ready when the stored view is cleared before defaults settle', () => {
        const controls: { setViewUrn?: (viewUrn: string | undefined) => void } = {};

        function ViewHarness({ children }: { children: React.ReactNode }) {
            const [selectedViewUrn, setViewUrn] = useState<string | undefined>(STORED_VIEW_URN);
            controls.setViewUrn = setViewUrn;
            const value: UserContextType = {
                loaded: true,
                state: DEFAULT_STATE,
                localState: { selectedViewUrn },
                updateLocalState: () => null,
                updateState: () => null,
                refetchUser: () => null,
            };
            return <UserContext.Provider value={value}>{children}</UserContext.Provider>;
        }

        const { result } = renderHook(() => useIsSearchViewReady(), { wrapper: ViewHarness });
        expect(result.current).toBe(true);

        act(() => {
            controls.setViewUrn?.(undefined);
        });
        expect(result.current).toBe(true);

        act(() => {
            vi.advanceTimersByTime(SEARCH_VIEW_READY_TIMEOUT_MS);
        });
        expect(result.current).toBe(true);
    });

    it('stays ready when a view arrives and is then cleared before defaults settle', () => {
        const controls: { setViewUrn?: (viewUrn: string | undefined) => void } = {};

        function ViewHarness({ children }: { children: React.ReactNode }) {
            const [selectedViewUrn, setViewUrn] = useState<string | undefined>(undefined);
            controls.setViewUrn = setViewUrn;
            const value: UserContextType = {
                loaded: true,
                state: DEFAULT_STATE,
                localState: { selectedViewUrn },
                updateLocalState: () => null,
                updateState: () => null,
                refetchUser: () => null,
            };
            return <UserContext.Provider value={value}>{children}</UserContext.Provider>;
        }

        const { result } = renderHook(() => useIsSearchViewReady(), { wrapper: ViewHarness });
        expect(result.current).toBe(false);

        act(() => {
            controls.setViewUrn?.(STORED_VIEW_URN);
        });
        expect(result.current).toBe(true);

        act(() => {
            controls.setViewUrn?.(undefined);
        });
        expect(result.current).toBe(true);
    });
});
