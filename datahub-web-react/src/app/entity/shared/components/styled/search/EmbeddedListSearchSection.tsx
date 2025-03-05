import React from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { ApolloError } from '@apollo/client';
import { FacetFilterInput } from '../../../../../../types.generated';
import useFilters from '../../../../../search/utils/useFilters';
import { navigateToEntitySearchUrl } from './navigateToEntitySearchUrl';
import { FilterSet, GetSearchResultsParams, SearchResultsInterface } from './types';
import { useEntityQueryParams } from '../../../containers/profile/utils';
import { EmbeddedListSearch } from './EmbeddedListSearch';
import { EMBEDDED_LIST_SEARCH_ENTITY_TYPES, UnionType } from '../../../../../search/utils/constants';
import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '../../../../../search/utils/types';

const FILTER = 'filter';

function getParamsWithoutFilters(params: QueryString.ParsedQuery<string>) {
    const paramsCopy = { ...params };
    Object.keys(paramsCopy).forEach((key) => {
        if (key.startsWith(FILTER)) {
            delete paramsCopy[key];
        }
    });
    return paramsCopy;
}

type Props = {
    emptySearchQuery?: string | null;
    fixedFilters?: FilterSet;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
    skipCache?: boolean;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
        refetch: (variables: GetSearchResultsParams['variables']) => Promise<SearchResultsInterface | undefined | null>;
    };
    useGetDownloadSearchResults?: (params: DownloadSearchResultsParams) => {
        loading: boolean;
        error: ApolloError | undefined;
        searchResults: DownloadSearchResults | undefined | null;
        refetch: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | undefined | null>;
    };
    shouldRefetch?: boolean;
    resetShouldRefetch?: () => void;
    applyView?: boolean;
    onLineageClick?: () => void;
    isLineageTab?: boolean;
};

export const EmbeddedListSearchSection = ({
    emptySearchQuery,
    fixedFilters,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    searchBarStyle,
    searchBarInputStyle,
    skipCache,
    useGetSearchResults,
    useGetDownloadSearchResults,
    shouldRefetch,
    resetShouldRefetch,
    applyView,
    onLineageClick,
    isLineageTab,
}: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityQueryParams = useEntityQueryParams();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsWithoutFilters = getParamsWithoutFilters(params);
    const baseParams = { ...entityQueryParams, ...paramsWithoutFilters };
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
            unionType,
            history,
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
            entityTypes={EMBEDDED_LIST_SEARCH_ENTITY_TYPES}
            query={query || ''}
            page={page}
            unionType={unionType}
            filters={filters}
            onChangeFilters={onChangeFilters}
            onChangeQuery={onSearch}
            onChangePage={onChangePage}
            onChangeUnionType={onChangeUnionType}
            emptySearchQuery={emptySearchQuery}
            fixedFilters={fixedFilters}
            fixedQuery={fixedQuery}
            placeholderText={placeholderText}
            defaultShowFilters={defaultShowFilters}
            defaultFilters={defaultFilters}
            searchBarStyle={searchBarStyle}
            searchBarInputStyle={searchBarInputStyle}
            skipCache={skipCache}
            useGetSearchResults={useGetSearchResults}
            useGetDownloadSearchResults={useGetDownloadSearchResults}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
            applyView={applyView}
            onLineageClick={onLineageClick}
            isLineageTab={isLineageTab}
        />
    );
};
