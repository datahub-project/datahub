import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EntityAndType } from '@app/entity/shared/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { ENTITY_PROFILE_V2_SIDEBAR_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import {
    SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
    SEARCH_RESULTS_FILTERS_ID,
    SEARCH_RESULTS_FILTERS_V2_INTRO,
} from '@app/onboarding/config/SearchOnboardingConfig';
import {
    ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
    ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
    ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
    ENTITY_SIDEBAR_V2_PROPERTIES_ID,
} from '@app/onboarding/configV2/EntityProfileOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import { SearchResults } from '@app/searchV2/SearchResults';
import SearchFiltersSection from '@app/searchV2/filters/SearchFiltersSection';
import useFilterMode from '@app/searchV2/filters/useFilterMode';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import { useIsBrowseV2, useIsSearchV2, useSearchVersion } from '@app/searchV2/useSearchAndBrowseVersion';
import { ENTITY_SUB_TYPE_FILTER_FIELDS, UnionType } from '@app/searchV2/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { getSearchCount } from '@app/searchV2/utils/searchUtils';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/searchV2/utils/types';
import { useDownloadScrollAcrossEntitiesSearchResults } from '@app/searchV2/utils/useDownloadScrollAcrossEntitiesSearchResults';
import { scrollToTop } from '@app/shared/searchUtils';
import { useAppConfig } from '@app/useAppConfig';
import { SearchCfg } from '@src/conf';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { FacetFilterInput } from '@types';

const Container = styled.span`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

/**
 * A search results page.
 */
export const SearchPage = () => {
    const { trackClearAllFiltersEvent } = useSearchFilterAnalytics();
    const { config } = useAppConfig();
    const showSearchFiltersV2 = useIsSearchV2();
    const showBrowseV2 = useIsBrowseV2();
    const searchVersion = useSearchVersion();
    const history = useHistory();
    const { query, unionType, filters, orFilters, viewUrn, page, sortInput } = useGetSearchQueryInputs();
    const { filterModeRef } = useFilterMode(filters, unionType);
    const selectedSortOption = useSelectedSortOption();

    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [isSelectMode, setIsSelectMode] = useState(false);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const start = (page - 1) * numResultsPerPage;

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
                start,
                count: getSearchCount(start, numResultsPerPage),
                filters: [],
                orFilters,
                viewUrn,
                sortInput,
                searchFlags: {
                    // TODO: Remove this before merging. For some reason causing issues locally.
                    // getSuggestions: true,
                    includeStructuredPropertyFacets: true,
                    skipHighlighting: config?.searchFlagsConfig?.defaultSkipHighlighting || false,
                },
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const total = data?.searchAcrossEntities?.total || 0;

    const searchResultEntities =
        data?.searchAcrossEntities?.searchResults?.map((result) => ({
            urn: result.entity.urn,
            type: result.entity.type,
        })) || [];
    const searchResultUrns = searchResultEntities.map((entity) => entity.urn);

    // This hook is simply used to generate a refetch callback that the DownloadAsCsv component can use to
    // download the correct results given the current context.
    // TODO: Use the loading indicator to log a message to the user should download to CSV fail.
    // TODO: Revisit this pattern -- what can we push down?
    const { refetch: refetchForDownload } = useDownloadScrollAcrossEntitiesSearchResults({
        variables: {
            input: {
                types: [],
                query,
                count: SearchCfg.RESULTS_PER_PAGE,
                orFilters,
                scrollId: null,
            },
        },
        skip: true,
    });

    const downloadSearchResults = (
        input: DownloadSearchResultsInput,
    ): Promise<DownloadSearchResults | null | undefined> => {
        return refetchForDownload(input);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({
            query,
            selectedSortOption,
            page: 1,
            filters: newFilters,
            history,
            unionType,
        });
    };

    const onClearFilters = () => {
        trackClearAllFiltersEvent(total);
        onChangeFilters([]);
    };

    const onChangeUnionType = (newUnionType: UnionType) => {
        navigateToSearchUrl({
            query,
            selectedSortOption,
            page: 1,
            filters,
            history,
            unionType: newUnionType,
        });
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        navigateToSearchUrl({
            query,
            selectedSortOption,
            page: newPage,
            filters,
            history,
            unionType,
        });
    };

    /**
     * Invoked when the "select all" checkbox is clicked.
     *
     * This method either adds the entire current page of search results to
     * the list of selected entities, or removes the current page from the set of selected entities.
     */
    const onChangeSelectAll = (selected: boolean) => {
        if (selected) {
            // Add current page of urns to the master selected entity list
            const entitiesToAdd = searchResultEntities.filter(
                (entity) =>
                    selectedEntities.findIndex(
                        (element) => element.urn === entity.urn && element.type === entity.type,
                    ) < 0,
            );
            setSelectedEntities(Array.from(new Set(selectedEntities.concat(entitiesToAdd))));
        } else {
            // Filter out the current page of entity urns from the list
            setSelectedEntities(selectedEntities.filter((entity) => searchResultUrns.indexOf(entity.urn) === -1));
        }
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
            filterCount: filters.length, // Only track changes to the filters, ignore toggling the mode by itself
            filterMode: filterModeRef.current,
            searchVersion,
        });
    }, [filters, filterModeRef, loading, query, searchVersion, total]);

    useEffect(() => {
        // When the query changes, then clear the select mode state
        setIsSelectMode(false);
    }, [query]);

    useEffect(() => {
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode]);

    // Render new search filters v2 onboarding step if the feature flag is on
    useUpdateEducationStepsAllowList(showSearchFiltersV2, SEARCH_RESULTS_FILTERS_V2_INTRO);

    // Render new browse v2 onboarding step if the feature flag is on
    useUpdateEducationStepsAllowList(showBrowseV2, SEARCH_RESULTS_BROWSE_SIDEBAR_ID);

    return (
        <Container>
            {!loading && (
                <OnboardingTour
                    stepIds={[
                        SEARCH_RESULTS_FILTERS_ID,
                        SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
                        SEARCH_RESULTS_FILTERS_V2_INTRO,
                        ENTITY_PROFILE_V2_SIDEBAR_ID,
                        ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
                        ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
                        ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
                        ENTITY_SIDEBAR_V2_PROPERTIES_ID,
                    ]}
                />
            )}
            <SearchFiltersSection
                loading={loading}
                availableFilters={data?.searchAcrossEntities?.facets || []}
                activeFilters={filters}
                unionType={unionType}
                onChangeFilters={onChangeFilters}
                onClearFilters={onClearFilters}
                onChangeUnionType={onChangeUnionType}
                query={query}
                viewUrn={viewUrn || undefined}
                totalResults={total}
                setShowSelectMode={setIsSelectMode}
                downloadSearchResults={downloadSearchResults}
            />
            <SearchResults
                page={page}
                query={query}
                error={error}
                searchResponse={data?.searchAcrossEntities}
                suggestions={data?.searchAcrossEntities?.suggestions || []}
                selectedFilters={filters}
                loading={loading}
                onChangeFilters={onChangeFilters}
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
        </Container>
    );
};
