import React, { useEffect, useState } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Alert } from 'antd';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput, EntityType } from '../../types.generated';
import useFilters from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { SearchResults } from './SearchResults';
import analytics, { EventType } from '../analytics';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { SearchCfg } from '../../conf';
import { ENTITY_FILTER_NAME } from './utils/constants';
import { GetSearchResultsParams } from '../entity/shared/components/styled/search/types';

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
    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .map((filter) => filter.value.toUpperCase() as EntityType);

    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                filters: filtersWithoutEntities,
            },
        },
    });

    // we need to extract refetch on its own so paging thru results for csv download
    // doesnt also update search results
    const { refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: filtersWithoutEntities,
            },
        },
    });

    const callSearchOnVariables = (variables: GetSearchResultsParams['variables']) => {
        return refetch(variables).then((res) => res.data.searchAcrossEntities);
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

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history });
    };

    const onChangePage = (newPage: number) => {
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history });
    };

    return (
        <>
            {!loading && error && (
                <Alert type="error" message={error?.message || `Search failed to load for query ${query}`} />
            )}
            <SearchResults
                entityFilters={entityFilters}
                filtersWithoutEntities={filtersWithoutEntities}
                callSearchOnVariables={callSearchOnVariables}
                page={page}
                query={query}
                searchResponse={data?.searchAcrossEntities}
                filters={data?.searchAcrossEntities?.facets}
                selectedFilters={filters}
                loading={loading}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                numResultsPerPage={numResultsPerPage}
                setNumResultsPerPage={setNumResultsPerPage}
            />
        </>
    );
};
