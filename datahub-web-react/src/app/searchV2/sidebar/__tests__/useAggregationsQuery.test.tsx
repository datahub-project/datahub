import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import { useSidebarFilters } from '@app/searchV2/sidebar/useSidebarFilters';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/searchV2/sidebar/useSidebarFilters', () => ({
    useSidebarFilters: vi.fn(),
}));

vi.mock('@app/searchV2/useGetSearchQueryInputs', () => ({
    default: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntity: () => ({ isBrowseEnabled: () => false }),
        getCollectionName: () => '',
        getDisplayName: () => '',
    }),
}));

vi.mock('@graphql/search.generated', () => ({
    useAggregateAcrossEntitiesQuery: vi.fn(),
}));

const useUserContextMock = vi.mocked(useUserContext);
const useSidebarFiltersMock = vi.mocked(useSidebarFilters);
const useGetSearchQueryInputsMock = vi.mocked(useGetSearchQueryInputs);
const useAggregateAcrossEntitiesQueryMock = vi.mocked(useAggregateAcrossEntitiesQuery);

function mockViews(
    hasSetDefaultView: boolean,
    selectedViewUrn: string | null | undefined,
    sidebarViewUrn: string | null | undefined,
) {
    useUserContextMock.mockReturnValue({
        state: { views: { hasSetDefaultView } },
        localState: { selectedViewUrn },
    } as ReturnType<typeof useUserContext>);
    useSidebarFiltersMock.mockReturnValue({
        entityFilters: [],
        query: 'events',
        orFilters: [],
        viewUrn: sidebarViewUrn,
    });
    useGetSearchQueryInputsMock.mockReturnValue({
        viewUrn: selectedViewUrn,
    } as ReturnType<typeof useGetSearchQueryInputs>);
}

function renderedSkip(excludeFilters = false) {
    renderHook(() => useAggregationsQuery({ facets: ['_entityType'], skip: false, excludeFilters }));
    const options = useAggregateAcrossEntitiesQueryMock.mock.calls.at(-1)?.[0];
    return options?.skip;
}

describe('useAggregationsQuery default view gate', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        useAggregateAcrossEntitiesQueryMock.mockReturnValue({
            data: undefined,
            previousData: undefined,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        } as unknown as ReturnType<typeof useAggregateAcrossEntitiesQuery>);
    });

    it('skips view-scoped aggregations until the default view is known', () => {
        mockViews(false, undefined, undefined);
        expect(renderedSkip()).toBe(true);
    });

    it('runs view-scoped aggregations once a view is stored and the sidebar filters match it', () => {
        mockViews(false, 'urn:li:dataHubView:stored', 'urn:li:dataHubView:stored');
        expect(renderedSkip()).toBe(false);
    });

    it('runs when the view was explicitly cleared', () => {
        mockViews(false, null, null);
        expect(renderedSkip()).toBe(false);
    });

    it('runs after resolution when there is no default view', () => {
        mockViews(true, undefined, undefined);
        expect(renderedSkip()).toBe(false);
    });

    it('waits while the sidebar is still holding the previous view', () => {
        mockViews(false, 'urn:li:dataHubView:stored', 'urn:li:dataHubView:previous');
        expect(renderedSkip()).toBe(true);
    });

    it('does not gate aggregations that do not apply a view', () => {
        mockViews(false, undefined, undefined);
        expect(renderedSkip(true)).toBe(false);
    });
});
