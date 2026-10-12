import { act, renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useSearchBarData } from '@app/searchV2/searchBarV2/hooks/useSearchBarData';

import { SearchBarApi } from '@types';

const DEBOUNCE_MS = 300;
const VIEW_URN = 'urn:li:dataHubView:default';

const { useAppConfigMock, useSelectedViewMock, autoCompleteLazyMock, searchLazyMock } = vi.hoisted(() => ({
    useAppConfigMock: vi.fn(),
    useSelectedViewMock: vi.fn(),
    autoCompleteLazyMock: vi.fn(),
    searchLazyMock: vi.fn(),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: useAppConfigMock,
}));

vi.mock('@app/searchV2/searchBarV2/hooks/useSelectedView', () => ({
    default: useSelectedViewMock,
}));

vi.mock('@src/graphql/search.generated', () => ({
    useGetAutoCompleteMultipleResultsLazyQuery: autoCompleteLazyMock,
    useGetSearchResultsForMultipleTrimmedLazyQuery: searchLazyMock,
}));

type HookProps = {
    query: string;
    isActive: boolean;
};

function elapseDebounce() {
    act(() => {
        vi.advanceTimersByTime(DEBOUNCE_MS);
    });
}

describe('useSearchBarData', () => {
    let getAutoComplete: ReturnType<typeof vi.fn>;
    let getSearchResults: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.useFakeTimers();
        getAutoComplete = vi.fn();
        getSearchResults = vi.fn();
        autoCompleteLazyMock.mockReturnValue([getAutoComplete, { data: undefined, loading: false }]);
        searchLazyMock.mockReturnValue([getSearchResults, { data: undefined, loading: false }]);
        useSelectedViewMock.mockReturnValue({ selectedView: undefined });
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    function renderSearchBarData(apiVariant: SearchBarApi, initialProps: HookProps) {
        useAppConfigMock.mockReturnValue({
            config: {
                searchBarConfig: { apiVariant },
                searchFlagsConfig: { defaultSkipHighlighting: false },
            },
        });

        return renderHook(
            (props: HookProps) => useSearchBarData(props.query, undefined, { isActive: props.isActive }),
            {
                initialProps,
            },
        );
    }

    function expectNoTypeahead() {
        expect(getAutoComplete).not.toHaveBeenCalled();
        expect(getSearchResults).not.toHaveBeenCalled();
    }

    function expectTypeahead(apiVariant: SearchBarApi, query: string, viewUrn: string | undefined) {
        if (apiVariant === SearchBarApi.SearchAcrossEntities) {
            expect(getSearchResults).toHaveBeenCalledWith({
                variables: {
                    input: {
                        query,
                        viewUrn,
                        orFilters: [],
                        count: 20,
                        searchFlags: { skipHighlighting: false },
                    },
                },
            });
            expect(getAutoComplete).not.toHaveBeenCalled();
            return;
        }

        expect(getAutoComplete).toHaveBeenCalledWith({
            variables: {
                input: {
                    query,
                    orFilters: [],
                    viewUrn,
                },
            },
        });
        expect(getSearchResults).not.toHaveBeenCalled();
    }

    it.each([SearchBarApi.AutocompleteForMultiple, SearchBarApi.SearchAcrossEntities])(
        'does not call either lazy query on mount, query changes, or selectedView resolution while inactive (%s)',
        (apiVariant) => {
            const { rerender } = renderSearchBarData(apiVariant, { query: 'foo', isActive: false });

            elapseDebounce();
            expect(getAutoComplete).not.toHaveBeenCalled();
            expect(getSearchResults).not.toHaveBeenCalled();

            rerender({ query: 'bars', isActive: false });
            elapseDebounce();
            expect(getAutoComplete).not.toHaveBeenCalled();
            expect(getSearchResults).not.toHaveBeenCalled();

            useSelectedViewMock.mockReturnValue({ selectedView: VIEW_URN });
            rerender({ query: 'bars', isActive: false });
            elapseDebounce();
            expect(getAutoComplete).not.toHaveBeenCalled();
            expect(getSearchResults).not.toHaveBeenCalled();
        },
    );

    it.each([SearchBarApi.AutocompleteForMultiple, SearchBarApi.SearchAcrossEntities])(
        'fetches the debounced query immediately when typeahead becomes active (%s)',
        (apiVariant) => {
            const { rerender } = renderSearchBarData(apiVariant, { query: 'foo', isActive: false });

            elapseDebounce();
            expectNoTypeahead();

            rerender({ query: 'foo', isActive: true });

            expectTypeahead(apiVariant, 'foo', undefined);
            const activeQuery = apiVariant === SearchBarApi.SearchAcrossEntities ? getSearchResults : getAutoComplete;
            expect(activeQuery).toHaveBeenCalledTimes(1);
        },
    );

    it.each([SearchBarApi.AutocompleteForMultiple, SearchBarApi.SearchAcrossEntities])(
        'does not fetch the previous query when activation and a query change happen together (%s)',
        (apiVariant) => {
            const { rerender } = renderSearchBarData(apiVariant, { query: 'foo', isActive: false });
            const activeQuery = apiVariant === SearchBarApi.SearchAcrossEntities ? getSearchResults : getAutoComplete;

            elapseDebounce();
            expect(getAutoComplete).not.toHaveBeenCalled();
            expect(getSearchResults).not.toHaveBeenCalled();

            rerender({ query: 'bars', isActive: true });
            expect(getAutoComplete).not.toHaveBeenCalled();
            expect(getSearchResults).not.toHaveBeenCalled();

            elapseDebounce();
            expectTypeahead(apiVariant, 'bars', undefined);
            expect(activeQuery).toHaveBeenCalledTimes(1);
        },
    );

    it.each([SearchBarApi.AutocompleteForMultiple, SearchBarApi.SearchAcrossEntities])(
        'fetches query and selected-view changes while active (%s)',
        (apiVariant) => {
            const { rerender } = renderSearchBarData(apiVariant, { query: 'foo', isActive: true });
            const activeQuery = apiVariant === SearchBarApi.SearchAcrossEntities ? getSearchResults : getAutoComplete;

            expectNoTypeahead();
            elapseDebounce();
            expectTypeahead(apiVariant, 'foo', undefined);
            expect(activeQuery).toHaveBeenCalledTimes(1);

            rerender({ query: 'bars', isActive: true });
            expect(activeQuery).toHaveBeenCalledTimes(1);

            elapseDebounce();
            expectTypeahead(apiVariant, 'bars', undefined);
            expect(activeQuery).toHaveBeenCalledTimes(2);

            useSelectedViewMock.mockReturnValue({ selectedView: VIEW_URN });
            rerender({ query: 'bars', isActive: true });
            expectTypeahead(apiVariant, 'bars', VIEW_URN);
            expect(activeQuery).toHaveBeenCalledTimes(3);
        },
    );
});
