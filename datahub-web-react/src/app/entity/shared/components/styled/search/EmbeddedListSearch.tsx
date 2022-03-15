import React, { useState } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { message } from 'antd';
import styled from 'styled-components';
import { ApolloError } from '@apollo/client';

import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput, FacetMetadata, Maybe, Scalars } from '../../../../../../types.generated';
import useFilters from '../../../../../search/utils/useFilters';
import { ENTITY_FILTER_NAME } from '../../../../../search/utils/constants';
import { SearchCfg } from '../../../../../../conf';
import { navigateToEntitySearchUrl } from './navigateToEntitySearchUrl';
import { EmbeddedListSearchResults } from './EmbeddedListSearchResults';
import EmbeddedListSearchHeader from './EmbeddedListSearchHeader';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { GetSearchResultsParams, SearchResultInterface } from './types';

const Container = styled.div`
    overflow: scroll;
    height: 120;
`;

// this extracts the response from useGetSearchResultsForMultipleQuery into a common interface other search endpoints can also produce
function useWrappedSearchResults(params: GetSearchResultsParams) {
    const { data, loading, error } = useGetSearchResultsForMultipleQuery(params);
    return { data: data?.searchAcrossEntities, loading, error };
}

type SearchPageParams = {
    type?: string;
};

type SearchResultsInterface = {
    /** The offset of the result set */
    start: Scalars['Int'];
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchResultInterface>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
};

type Props = {
    emptySearchQuery?: string | null;
    fixedFilter?: FacetFilterInput | null;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
    };
};

export const EmbeddedListSearch = ({
    emptySearchQuery,
    fixedFilter,
    fixedQuery,
    placeholderText,
    useGetSearchResults = useWrappedSearchResults,
}: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = params.query ? (params.query as string) : '';
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const finalFilters = (fixedFilter && [...filtersWithoutEntities, fixedFilter]) || filtersWithoutEntities;
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .map((filter) => filter.value.toUpperCase() as EntityType);

    const [showFilters, setShowFilters] = useState(false);

    const { data, loading, error } = useGetSearchResults({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: finalFilters,
            },
        },
    });

    const onSearch = (q: string) => {
        let finalQuery = ``;
        if (q.trim().length === 0) {
            if (emptySearchQuery) {
                finalQuery = emptySearchQuery;
            } else {
                return;
            }
        } else {
            if (fixedQuery) {
                finalQuery = `(${q}) AND (${fixedQuery})`;
            }
            finalQuery = q;
        }
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query: finalQuery,
            page: 1,
            history,
        });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query,
            page: 1,
            filters: newFilters,
            history,
        });
    };

    const onChangePage = (newPage: number) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            type: activeType,
            query,
            page: newPage,
            filters,
            history,
        });
    };

    const toggleFilters = () => {
        setShowFilters(!showFilters);
    };

    // Filter out the persistent filter values
    const filteredFilters = data?.facets?.filter((facet) => facet.field !== fixedFilter?.field) || [];

    return (
        <Container>
            {error && message.error(`Failed to complete search: ${error && error.message}`)}
            <EmbeddedListSearchHeader
                onSearch={onSearch}
                placeholderText={placeholderText}
                onToggleFilters={toggleFilters}
            />
            <EmbeddedListSearchResults
                loading={loading}
                searchResponse={data}
                filters={filteredFilters}
                selectedFilters={filters}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                page={page}
                showFilters={showFilters}
            />
        </Container>
    );
};
