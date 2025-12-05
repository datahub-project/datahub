import { useEffect, useMemo, useState } from 'react';

import useSearchResultsOptions from '@app/searchV2/searchBarV2/hooks/useAutocompleteSuggestionsOptions';
import useRecentlySearchedQueriesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlySearchedQueriesOptions';
import useRecentlyViewedEntitiesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntitiesOptions';
import useViewAllResultsOptions from '@app/searchV2/searchBarV2/hooks/useViewAllResultsOptions';
import { EntityWithMatchedFields } from '@app/searchV2/utils/combineSiblingsInEntitiesWithMatchedFields';
import usePrevious from '@app/shared/usePrevious';
import { useIsAiChatEnabled } from '@app/useAppConfig';

export default function useOptions(
    searchQuery: string,
    showViewAllResults: boolean | undefined,
    entitiesWithMatchedFields: EntityWithMatchedFields[] | undefined,
    isDataLoading: boolean,
    shouldCombineSiblings: boolean,
    isSearching: boolean,
    shouldShowAutoCompleteResults: boolean,
    skipRecommendations?: boolean,
) {
    // used to show Loader when we searching for suggestions in both cases for the first time and after clearing searchQuery
    const [isDataInitialized, setIsDataInitialized] = useState<boolean>(false);
    const isAskDataHubEnabled = useIsAiChatEnabled();

    const hasResults = useMemo(() => (entitiesWithMatchedFields?.length ?? 0) > 0, [entitiesWithMatchedFields?.length]);

    useEffect(() => {
        if (searchQuery === '') setIsDataInitialized(false);
    }, [searchQuery]);

    const previousIsLoading = usePrevious(isDataLoading);
    useEffect(() => {
        if (previousIsLoading && !isDataLoading) {
            setIsDataInitialized(true);
        }
    }, [isDataLoading, previousIsLoading]);

    const recentlySearchedQueriesOptions = useRecentlySearchedQueriesOptions(skipRecommendations);
    const recentlyViewedEntitiesOptions = useRecentlyViewedEntitiesOptions(skipRecommendations);

    const initialOptions = useMemo(() => {
        return [...recentlyViewedEntitiesOptions, ...recentlySearchedQueriesOptions];
    }, [recentlyViewedEntitiesOptions, recentlySearchedQueriesOptions]);

    const viewAllResultsOptions = useViewAllResultsOptions(searchQuery, showViewAllResults);

    const searchResultsOptions = useSearchResultsOptions(
        entitiesWithMatchedFields,
        searchQuery,
        isDataLoading,
        isDataInitialized,
        shouldCombineSiblings,
    );

    const options = useMemo(() => {
        if (!isSearching) return initialOptions;

        if (shouldShowAutoCompleteResults) {
            if (!isDataLoading && !hasResults && isDataInitialized) {
                // When there are no results and Ask DataHub is enabled, show viewAllResultsOptions (e.g., "Ask DataHub")
                // to avoid showing "no results found". Otherwise, return empty array to show "no results found".
                return isAskDataHubEnabled ? viewAllResultsOptions : [];
            }
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
        isDataInitialized,
        isAskDataHubEnabled,
    ]);

    return options;
}
