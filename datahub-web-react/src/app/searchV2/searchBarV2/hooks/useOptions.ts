import { Entity } from '@src/types.generated';
import { useMemo } from 'react';
import useSearchResultsOptions from '@app/searchV2/searchBarV2/hooks/useAutocompleteSuggestionsOptions';
import useRecentlySearchedQueriesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlySearchedQueriesOptions';
import useRecentlyViewedEntitiesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntitiesOptions';
import useViewAllResultsOptions from '@app/searchV2/searchBarV2/hooks/useViewAllResultsOptions';

export default function useOptions(
    searchQuery: string,
    showViewAllResults: boolean | undefined,
    entities: Entity[] | undefined,
    isDataLoading: boolean,
    isDataInitialized: boolean,
    shouldCombineSiblings: boolean,
    isSearching: boolean,
    shouldShowAutoCompleteResults: boolean,
) {
    const recentlySearchedQueriesOptions = useRecentlySearchedQueriesOptions();
    const recentlyViewedEntitiesOptions = useRecentlyViewedEntitiesOptions();

    const initialOptions = useMemo(() => {
        return [...recentlyViewedEntitiesOptions, ...recentlySearchedQueriesOptions];
    }, [recentlyViewedEntitiesOptions, recentlySearchedQueriesOptions]);

    const viewAllResultsOptions = useViewAllResultsOptions(searchQuery, showViewAllResults);

    const hasResults = useMemo(() => (entities?.length ?? 0) > 0, [entities?.length]);

    const searchResultsOptions = useSearchResultsOptions(
        entities,
        searchQuery,
        isDataLoading,
        isDataInitialized,
        shouldCombineSiblings,
    );

    const options = useMemo(() => {
        if (!isSearching) return initialOptions;

        if (shouldShowAutoCompleteResults) {
            if (!isDataLoading && !hasResults) return [];
            return [...viewAllResultsOptions, ...searchResultsOptions];
        }

        return [];
    }, [
        isSearching,
        hasResults,
        initialOptions,
        searchResultsOptions,
        viewAllResultsOptions,
        shouldShowAutoCompleteResults,
        isDataLoading,
    ]);

    return options;
}
