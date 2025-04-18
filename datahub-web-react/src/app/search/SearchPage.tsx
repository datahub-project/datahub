import React, { useEffect, useState } from 'react';
import { SearchResults } from './SearchResults';
import analytics, { EventType } from '../analytics';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { SearchCfg } from '../../conf';
import { ENTITY_SUB_TYPE_FILTER_FIELDS } from './utils/constants';
import { EntityAndType } from '../entity/shared/types';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    SEARCH_RESULTS_ADVANCED_SEARCH_ID,
    SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
    SEARCH_RESULTS_FILTERS_ID,
    SEARCH_RESULTS_FILTERS_V2_INTRO,
} from '../onboarding/config/SearchOnboardingConfig';
import { SearchFilters } from './filters/SearchFilters';
import useGetSearchQueryInputs from './useGetSearchQueryInputs';
import useSearchFilterAnalytics from './filters/useSearchFilterAnalytics';
import { useIsBrowseV2, useIsSearchV2, useSearchVersion } from './useSearchAndBrowseVersion';
import useFilterMode from './filters/useFilterMode';
import { useToggleEducationStepIdsAllowList } from '../onboarding/useToggleEducationStepIdsAllowList';
import useSearchPage from './useSearchPage';

/**
 * A search results page.
 */
export const SearchPage = () => {
    const { trackClearAllFiltersEvent } = useSearchFilterAnalytics();
    const showSearchFiltersV2 = useIsSearchV2();
    const showBrowseV2 = useIsBrowseV2();
    const searchVersion = useSearchVersion();
    const { query, unionType, filters, orFilters, viewUrn, page, sortInput } = useGetSearchQueryInputs();
    const { filterMode, filterModeRef, setFilterMode } = useFilterMode(filters, unionType);

    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);

    const {
        data,
        loading,
        error,
        refetch: realRefetch,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [],
                query,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                filters: [],
                orFilters,
                viewUrn,
                sortInput,
                searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const total = data?.searchAcrossEntities?.total || 0;
    const {
        isSelectMode,
        setIsSelectMode,
        downloadSearchResults,
        onChangeFilters,
        onChangeUnionType,
        onChangePage,
        onChangeSelectAll,
    } = useSearchPage({
        searchResults: data?.searchAcrossEntities?.searchResults || [],
        selectedEntities,
        setSelectedEntities,
    });

    const onClearFilters = () => {
        trackClearAllFiltersEvent(total);
        onChangeFilters([]);
    };

    useEffect(() => {
        if (loading) return;

        const entityTypes = Array.from(
            new Set(
                filters
                    .filter((filter) => ENTITY_SUB_TYPE_FILTER_FIELDS.includes(filter.field))
                    .flatMap((filter) => filter.values ?? []),
            ),
        );

        const filterFields = Array.from(new Set(filters.map((filter) => filter.field)));

        analytics.event({
            type: EventType.SearchResultsViewEvent,
            query,
            total,
            entityTypes,
            filterFields,
            filterCount: filters.length,
            // Only track changes to the filters, ignore toggling the mode by itself
            filterMode: filterModeRef.current,
            searchVersion,
        });
    }, [filters, filterModeRef, loading, query, searchVersion, total]);

    useEffect(() => {
        // When the query changes, then clear the select mode state
        setIsSelectMode(false);
    }, [query, setIsSelectMode]);

    useEffect(() => {
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode]);

    // Render new search filters v2 onboarding step if the feature flag is on
    useToggleEducationStepIdsAllowList(showSearchFiltersV2, SEARCH_RESULTS_FILTERS_V2_INTRO);

    // Render new browse v2 onboarding step if the feature flag is on
    useToggleEducationStepIdsAllowList(showBrowseV2, SEARCH_RESULTS_BROWSE_SIDEBAR_ID);

    return (
        <>
            {!loading && (
                <OnboardingTour
                    stepIds={[
                        SEARCH_RESULTS_FILTERS_ID,
                        SEARCH_RESULTS_ADVANCED_SEARCH_ID,
                        SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
                        SEARCH_RESULTS_FILTERS_V2_INTRO,
                    ]}
                />
            )}
            {showSearchFiltersV2 && (
                <SearchFilters
                    loading={loading}
                    availableFilters={data?.searchAcrossEntities?.facets || []}
                    activeFilters={filters}
                    unionType={unionType}
                    mode={filterMode}
                    onChangeFilters={onChangeFilters}
                    onClearFilters={onClearFilters}
                    onChangeUnionType={onChangeUnionType}
                    onChangeMode={setFilterMode}
                />
            )}
            <SearchResults
                unionType={unionType}
                downloadSearchResults={downloadSearchResults}
                page={page}
                query={query}
                viewUrn={viewUrn || undefined}
                error={error}
                searchResponse={data?.searchAcrossEntities}
                facets={data?.searchAcrossEntities?.facets}
                suggestions={data?.searchAcrossEntities?.suggestions || []}
                selectedFilters={filters}
                loading={loading}
                onChangeFilters={onChangeFilters}
                onChangeUnionType={onChangeUnionType}
                onChangePage={onChangePage}
                numResultsPerPage={numResultsPerPage}
                setNumResultsPerPage={setNumResultsPerPage}
                isSelectMode={isSelectMode}
                selectedEntities={selectedEntities}
                setSelectedEntities={setSelectedEntities}
                setIsSelectMode={setIsSelectMode}
                onChangeSelectAll={onChangeSelectAll}
                refetch={realRefetch}
            />
        </>
    );
};
