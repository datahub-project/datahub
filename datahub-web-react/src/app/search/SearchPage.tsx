import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import { FacetFilterInput } from '../../types.generated';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { SearchResults } from './SearchResults';
import analytics, { EventType } from '../analytics';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { SearchCfg } from '../../conf';
import { UnionType } from './utils/constants';
import { EntityAndType } from '../entity/shared/types';
import { scrollToTop } from '../shared/searchUtils';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    FILTERS_V2_CONDITIONAL_STEP,
    SEARCH_RESULTS_ADVANCED_SEARCH_ID,
    SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
    SEARCH_RESULTS_FILTERS_ID,
} from '../onboarding/config/SearchOnboardingConfig';
import { useDownloadScrollAcrossEntitiesSearchResults } from './utils/useDownloadScrollAcrossEntitiesSearchResults';
import { DownloadSearchResults, DownloadSearchResultsInput } from './utils/types';
import SearchFilters from './filters/SearchFilters';
import useGetSearchQueryInputs from './useGetSearchQueryInputs';
import { useAppConfig } from '../useAppConfig';

/**
 * A search results page.
 */
export const SearchPage = () => {
    const appConfig = useAppConfig();
    const history = useHistory();
    const { query, unionType, filters, orFilters, viewUrn, page, activeType } = useGetSearchQueryInputs();

    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);
    const [isSelectMode, setIsSelectMode] = useState(false);
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
            },
        },
    });

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
        navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history, unionType });
    };

    const onChangeUnionType = (newUnionType: UnionType) => {
        navigateToSearchUrl({ type: activeType, query, page: 1, filters, history, unionType: newUnionType });
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history, unionType });
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
        if (!loading) {
            analytics.event({
                type: EventType.SearchResultsViewEvent,
                query,
                total: data?.searchAcrossEntities?.count || 0,
            });
        }
    }, [query, data, loading]);

    useEffect(() => {
        // When the query changes, then clear the select mode state
        setIsSelectMode(false);
    }, [query]);

    useEffect(() => {
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode]);

    const { showSearchFiltersV2 } = appConfig.config.featureFlags;

    return (
        <>
            {!loading && (
                <OnboardingTour
                    stepIds={[
                        SEARCH_RESULTS_FILTERS_ID,
                        SEARCH_RESULTS_ADVANCED_SEARCH_ID,
                        SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
                    ]}
                    conditionalSteps={[FILTERS_V2_CONDITIONAL_STEP]}
                />
            )}
            {showSearchFiltersV2 && (
                <SearchFilters
                    availableFilters={data?.searchAcrossEntities?.facets || []}
                    activeFilters={filters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                    onChangeUnionType={onChangeUnionType}
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
