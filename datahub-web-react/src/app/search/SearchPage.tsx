import React, { useEffect } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { Alert } from 'antd';

import { SearchablePage } from './SearchablePage';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput, EntityType } from '../../types.generated';
import useFilters, { useEntityFilters } from './utils/useFilters';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { AllEntitiesSearchResults } from './AllEntitiesSearchResults';
import analytics, { EventType } from '../analytics';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';

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
    const query: string = params.query ? (params.query as string) : '';
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const filters: Array<FacetFilterInput> = useFilters(params, true);
    const filtersWithoutEntities: Array<FacetFilterInput> = useFilters(params, false);
    const entityFilters: Array<EntityType> = useEntityFilters(params);

    const { data, loading, error } = useGetSearchResultsQuery({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: 0,
                count: 20,
                filters: filtersWithoutEntities,
            },
        },
    });

    useEffect(() => {
        if (!loading) {
            analytics.event({
                type: EventType.SearchResultsViewEvent,
                query,
                total: data?.search?.count || 0,
            });
        }
    }, [query, data, loading]);

    const onSearch = (q: string, type?: EntityType) => {
        if (q.trim().length === 0) {
            return;
        }
        analytics.event({
            type: EventType.SearchEvent,
            query: q,
            entityTypeFilter: activeType,
            pageNumber: 1,
            originPath: window.location.pathname,
        });
        navigateToSearchUrl({ type: type || activeType, query: q, page: 1, history, entityRegistry });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({ type: activeType, query, page: 1, filters: newFilters, history, entityRegistry });
    };

    const onChangePage = (newPage: number) => {
        navigateToSearchUrl({ type: activeType, query, page: newPage, filters, history, entityRegistry });
    };

    return (
        <SearchablePage initialQuery={query} onSearch={onSearch}>
            {!loading && error && (
                <Alert type="error" message={error?.message || `Search failed to load for query ${query}`} />
            )}
            <AllEntitiesSearchResults
                query={query}
                searchResults={data?.search?.searchResults}
                filters={data?.search?.facets}
                selectedFilters={filters}
                loading={loading}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
            />
        </SearchablePage>
    );
};
