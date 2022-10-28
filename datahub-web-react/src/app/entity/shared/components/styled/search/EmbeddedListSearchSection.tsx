import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { ApolloError } from '@apollo/client';
import { FacetFilterInput } from '../../../../../../types.generated';
import useFilters from '../../../../../search/utils/useFilters';
import { navigateToEntitySearchUrl } from './navigateToEntitySearchUrl';
import { GetSearchResultsParams, SearchResultsInterface } from './types';
import { useEntityQueryParams } from '../../../containers/profile/utils';
import { EmbeddedListSearch } from './EmbeddedListSearch';
import { UnionType } from '../../../../../search/utils/constants';

type Props = {
    emptySearchQuery?: string | null;
    fixedFilter?: FacetFilterInput | null;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
        refetch: (variables: GetSearchResultsParams['variables']) => Promise<SearchResultsInterface | undefined | null>;
    };
};

export const EmbeddedListSearchSection = ({
    emptySearchQuery,
    fixedFilter,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    searchBarStyle,
    searchBarInputStyle,
    useGetSearchResults,
}: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityQueryParams = useEntityQueryParams();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const baseParams = { ...params, ...entityQueryParams };
    const query: string = params?.query as string;
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;

    const filters: Array<FacetFilterInput> = useFilters(params);

    const onSearch = (q: string) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
            query: q,
            page: 1,
            filters,
            history,
            unionType,
        });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
            query,
            page: 1,
            filters: newFilters,
            history,
            unionType,
        });
    };

    const onChangePage = (newPage: number) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
            query,
            page: newPage,
            filters,
            history,
            unionType,
        });
    };

    const onChangeUnionType = (newUnionType: UnionType) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
            query,
            page,
            filters,
            history,
            unionType: newUnionType,
        });
    };

    return (
        <EmbeddedListSearch
            query={query || ''}
            page={page}
            unionType={unionType}
            filters={filters}
            onChangeFilters={onChangeFilters}
            onChangeQuery={onSearch}
            onChangePage={onChangePage}
            onChangeUnionType={onChangeUnionType}
            emptySearchQuery={emptySearchQuery}
            fixedFilter={fixedFilter}
            fixedQuery={fixedQuery}
            placeholderText={placeholderText}
            defaultShowFilters={defaultShowFilters}
            defaultFilters={defaultFilters}
            searchBarStyle={searchBarStyle}
            searchBarInputStyle={searchBarInputStyle}
            useGetSearchResults={useGetSearchResults}
        />
    );
};
