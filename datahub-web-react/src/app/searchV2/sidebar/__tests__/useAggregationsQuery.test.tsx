import { act, renderHook } from '@testing-library/react-hooks';
import React, { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext, UserContextType } from '@app/context/userContext';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';

const STORED_VIEW_URN = 'urn:li:dataHubView:default';

const { liveInputs, aggregateCalls } = vi.hoisted(() => ({
    liveInputs: {
        viewUrn: undefined as string | null | undefined,
        entityFilters: [] as string[],
        query: 'events',
        orFilters: [] as unknown[],
    },
    aggregateCalls: [] as Array<{
        skip: boolean;
        viewUrn: string | null | undefined;
        liveViewUrn: string | null | undefined;
    }>,
}));

vi.mock('@app/searchV2/sidebar/BrowseContext', () => ({
    useMaybeEntityType: () => undefined,
    useMaybeEnvironmentAggregation: () => undefined,
    useMaybePlatformAggregation: () => undefined,
}));

vi.mock('@app/searchV2/useGetSearchQueryInputs', () => ({
    default: () => liveInputs,
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntity: () => ({ isBrowseEnabled: () => true }),
        getCollectionName: (value: string) => value,
        getDisplayName: () => '',
    }),
}));

vi.mock('@graphql/search.generated', () => ({
    useAggregateAcrossEntitiesQuery: (options: {
        skip: boolean;
        variables: { input: { viewUrn?: string | null } };
    }) => {
        aggregateCalls.push({
            skip: options.skip,
            viewUrn: options.variables.input.viewUrn,
            liveViewUrn: liveInputs.viewUrn,
        });
        return { data: undefined, loading: false, error: undefined, refetch: vi.fn() };
    },
}));

function userContextFor(viewUrn: string | null | undefined): UserContextType {
    return {
        loaded: true,
        state: {
            ...DEFAULT_STATE,
            views: {
                ...DEFAULT_STATE.views,
                hasSetDefaultView: viewUrn !== undefined,
            },
        },
        localState: { selectedViewUrn: viewUrn },
        updateLocalState: () => null,
        updateState: () => null,
        refetchUser: () => null,
    };
}

function renderAggregations(excludeFilters = false) {
    const controls: { setViewUrn?: (viewUrn: string) => void } = {};
    function ViewHarness({ children }: { children: React.ReactNode }) {
        const [viewUrn, setViewUrn] = useState<string | undefined>(undefined);
        controls.setViewUrn = setViewUrn;
        liveInputs.viewUrn = viewUrn;
        return <UserContext.Provider value={userContextFor(viewUrn)}>{children}</UserContext.Provider>;
    }
    const rendered = renderHook(() => useAggregationsQuery({ facets: ['entity'], skip: false, excludeFilters }), {
        wrapper: ViewHarness,
    });
    return { ...rendered, controls };
}

describe('useAggregationsQuery view copy', () => {
    beforeEach(() => {
        aggregateCalls.length = 0;
        liveInputs.viewUrn = undefined;
    });

    it('does not aggregate without a view while the sidebar copy still has none', () => {
        const { controls } = renderAggregations(false);

        expect(aggregateCalls.every((call) => call.skip)).toBe(true);

        act(() => {
            controls.setViewUrn?.(STORED_VIEW_URN);
        });

        const callsWithLiveView = aggregateCalls.filter((call) => call.liveViewUrn === STORED_VIEW_URN);
        const unskipped = callsWithLiveView.filter((call) => !call.skip);

        // The first render that sees the default view still has the previous sidebar copy.
        expect(callsWithLiveView[0]?.skip).toBe(true);
        expect(callsWithLiveView[0]?.viewUrn).toBeUndefined();
        expect(unskipped.length).toBeGreaterThan(0);
        expect(unskipped.every((call) => call.viewUrn === STORED_VIEW_URN)).toBe(true);
    });

    it('runs the base aggregation without waiting for the sidebar view copy', () => {
        const { controls } = renderAggregations(true);
        const callsBeforeView = aggregateCalls.length;

        act(() => {
            controls.setViewUrn?.(STORED_VIEW_URN);
        });

        const firstCallAfterView = aggregateCalls[callsBeforeView];
        expect(firstCallAfterView?.liveViewUrn).toBe(STORED_VIEW_URN);
        expect(firstCallAfterView?.skip).toBe(false);
        expect(firstCallAfterView?.viewUrn).toBeUndefined();
    });
});
