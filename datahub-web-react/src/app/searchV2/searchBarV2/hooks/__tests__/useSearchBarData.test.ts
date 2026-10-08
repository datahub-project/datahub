import { act, renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { SearchBarDataOptions, useSearchBarData } from '@app/searchV2/searchBarV2/hooks/useSearchBarData';
import { useAppConfig } from '@app/useAppConfig';
import { SearchBarApi } from '@src/types.generated';

const { search, autocomplete, selectedViewState } = vi.hoisted(() => ({
    search: vi.fn(),
    autocomplete: vi.fn(),
    selectedViewState: { urn: undefined as string | null | undefined },
}));

vi.mock('@src/graphql/search.generated', () => ({
    useGetSearchResultsForMultipleTrimmedLazyQuery: () => [search, { data: undefined, loading: false }],
    useGetAutoCompleteMultipleResultsLazyQuery: () => [autocomplete, { data: undefined, loading: false }],
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn(),
}));

vi.mock('@app/searchV2/searchBarV2/hooks/useSelectedView', () => ({
    default: () => ({ selectedView: selectedViewState.urn }),
}));

const useAppConfigMock = vi.mocked(useAppConfig);
const PAGE_QUERY = 'events';

type HookProps = {
    query: string;
    filters?: Map<string, { filters: Array<{ field: string; values: string[] }> }>;
    options: SearchBarDataOptions;
};

function renderSearchBarData(initial: HookProps) {
    return renderHook((props: HookProps) => useSearchBarData(props.query, props.filters, props.options), {
        initialProps: initial,
    });
}

function settleDebounce() {
    act(() => {
        vi.advanceTimersByTime(300);
    });
}

describe('useSearchBarData', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        search.mockClear();
        autocomplete.mockClear();
        selectedViewState.urn = undefined;
        useAppConfigMock.mockReturnValue({
            config: {
                searchBarConfig: { apiVariant: SearchBarApi.SearchAcrossEntities },
                searchFlagsConfig: { defaultSkipHighlighting: false },
            },
        } as ReturnType<typeof useAppConfig>);
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('does not fetch on mount or while the bar stays inactive', () => {
        const { rerender } = renderSearchBarData({
            query: PAGE_QUERY,
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();

        expect(search).not.toHaveBeenCalled();
        expect(autocomplete).not.toHaveBeenCalled();

        rerender({
            query: 'events_v2',
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: true },
        });
        settleDebounce();

        expect(search).not.toHaveBeenCalled();
        expect(autocomplete).not.toHaveBeenCalled();
    });

    it('does not fetch when the bar is focused on the page-open query', () => {
        const { rerender } = renderSearchBarData({
            query: PAGE_QUERY,
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();

        rerender({
            query: PAGE_QUERY,
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();

        expect(search).not.toHaveBeenCalled();
    });

    it('fetches when the bar becomes active with a query that differs from the page-open query', () => {
        const { rerender } = renderSearchBarData({
            query: 'events_v2',
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: true },
        });
        settleDebounce();
        expect(search).not.toHaveBeenCalled();

        rerender({
            query: 'events_v2',
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: true },
        });
        settleDebounce();

        expect(search).toHaveBeenCalledTimes(1);
        expect(search).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ query: 'events_v2' }),
                }),
            }),
        );
    });

    it('fetches again when the query changes while the bar is active', () => {
        const { rerender } = renderSearchBarData({
            query: 'events_v2',
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: true },
        });
        settleDebounce();
        expect(search).toHaveBeenCalledTimes(1);

        rerender({
            query: 'events_v3',
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: true },
        });
        settleDebounce();

        expect(search).toHaveBeenCalledTimes(2);
        expect(search).toHaveBeenLastCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ query: 'events_v3' }),
                }),
            }),
        );
    });

    it('fetches when filters change while the bar is in use', () => {
        const { rerender } = renderSearchBarData({
            query: PAGE_QUERY,
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();
        expect(search).not.toHaveBeenCalled();

        rerender({
            query: PAGE_QUERY,
            filters: new Map([['platform', { filters: [{ field: 'platform', values: ['hive'] }] }]]),
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();

        expect(search).toHaveBeenCalledTimes(1);
        expect(search).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        query: PAGE_QUERY,
                        orFilters: [{ and: [{ field: 'platform', values: ['hive'] }] }],
                    }),
                }),
            }),
        );
    });

    it('fetches on the next focus after the view changes while the bar is inactive', () => {
        const { rerender } = renderSearchBarData({
            query: PAGE_QUERY,
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();
        expect(search).not.toHaveBeenCalled();

        selectedViewState.urn = 'urn:li:dataHubView:team';
        rerender({
            query: PAGE_QUERY,
            options: { isActive: false, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();
        expect(search).not.toHaveBeenCalled();

        rerender({
            query: PAGE_QUERY,
            options: { isActive: true, initialQuery: PAGE_QUERY, hasUserTyped: false },
        });
        settleDebounce();

        expect(search).toHaveBeenCalledTimes(1);
        expect(search).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        query: PAGE_QUERY,
                        viewUrn: 'urn:li:dataHubView:team',
                    }),
                }),
            }),
        );
    });

    it('gates autocomplete the same way when that api variant is configured', () => {
        useAppConfigMock.mockReturnValue({
            config: {
                searchBarConfig: { apiVariant: SearchBarApi.AutocompleteForMultiple },
                searchFlagsConfig: { defaultSkipHighlighting: false },
            },
        } as ReturnType<typeof useAppConfig>);

        const { rerender } = renderSearchBarData({
            query: 'e',
            options: { isActive: false, initialQuery: 'e', hasUserTyped: false },
        });
        settleDebounce();
        expect(autocomplete).not.toHaveBeenCalled();
        expect(search).not.toHaveBeenCalled();

        rerender({
            query: 'ev',
            options: { isActive: true, initialQuery: 'e', hasUserTyped: true },
        });
        settleDebounce();

        expect(search).not.toHaveBeenCalled();
        expect(autocomplete).toHaveBeenCalledTimes(1);
        expect(autocomplete).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ query: 'ev' }),
                }),
            }),
        );
    });
});
