import React, { useEffect, useState } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput, EntityType } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { SearchResults } from './SearchResults';
import analytics, { EventType } from '../analytics';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { SearchCfg } from '../../conf';
import { ENTITY_FILTER_NAME, UnionType } from './utils/constants';
import { GetSearchResultsParams } from '../entity/shared/components/styled/search/types';
import { EntityAndType } from '../entity/shared/types';
import { scrollToTop } from '../shared/searchUtils';
import { generateOrFilters } from './utils/generateOrFilters';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    SEARCH_RESULTS_ADVANCED_SEARCH_ID,
    SEARCH_RESULTS_FILTERS_ID,
} from '../onboarding/config/SearchOnboardingConfig';

type SearchPageParams = {
    type?: string;
};

/**
 * A search results page.
 */
export const SearchPage = () => {
    const history = useHistory();
    const location = useLocation();

    const entityRegistry = useEntityRegistry();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;

    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => filter.values?.map((value) => value?.toUpperCase() as EntityType) || []);

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
                types: entityFilters,
                query,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                filters: [],
                orFilters: generateOrFilters(unionType, filtersWithoutEntities),
            },
        },
    });

    const searchResultEntities =
        data?.searchAcrossEntities?.searchResults?.map((result) => ({
            urn: result.entity.urn,
            type: result.entity.type,
        })) || [];
    const searchResultUrns = searchResultEntities.map((entity) => entity.urn);

    // we need to extract refetch on its own so paging thru results for csv download
    // doesnt also update search results
    const { refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: [],
                orFilters: generateOrFilters(unionType, filtersWithoutEntities),
            },
        },
    });

    const callSearchOnVariables = (variables: GetSearchResultsParams['variables']) => {
        return refetch(variables).then((res) => res.data.searchAcrossEntities);
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

    return (
        <>
            {!loading && <OnboardingTour stepIds={[SEARCH_RESULTS_FILTERS_ID, SEARCH_RESULTS_ADVANCED_SEARCH_ID]} />}
            <SearchResults
                unionType={unionType}
                entityFilters={entityFilters}
                filtersWithoutEntities={filtersWithoutEntities}
                callSearchOnVariables={callSearchOnVariables}
                page={page}
                query={query}
                error={error}
                searchResponse={data?.searchAcrossEntities}
                filters={data?.searchAcrossEntities?.facets}
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
