import { ApolloError } from '@apollo/client';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory, useLocation } from 'react-router';

import { EmbeddedListSearch } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearch';
import { navigateToEntitySearchUrl } from '@app/entityV2/shared/components/styled/search/navigateToEntitySearchUrl';
import {
    FilterSet,
    GetSearchResultsParams,
    SearchResultsInterface,
} from '@app/entityV2/shared/components/styled/search/types';
import { useEntityQueryParams } from '@app/entityV2/shared/containers/profile/utils';
import { decodeComma } from '@app/entityV2/shared/utils';
import {
    EXTRA_EMBEDDED_LIST_SEARCH_ENTITY_TYPES_TO_SUPPLEMENT_SEARCHABLE_ENTITY_TYPES,
    UnionType,
} from '@app/search/utils/constants';
import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '@app/search/utils/types';
import useFilters from '@app/search/utils/useFilters';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { useSelectedSortOption } from '@src/app/search/context/SearchContext';
import useSortInput from '@src/app/searchV2/sorting/useSortInput';

import { FacetFilterInput } from '@types';

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
    useGetSearchCountResult?: (params: GetSearchResultsParams) => {
        total: number | undefined;
        loading: boolean;
        error?: ApolloError;
    };
    shouldRefetch?: boolean;
    resetShouldRefetch?: () => void;
    applyView?: boolean;
    showFilterBar?: boolean;
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
    useGetSearchCountResult,
    shouldRefetch,
    resetShouldRefetch,
    applyView,
    showFilterBar,
}: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityQueryParams = useEntityQueryParams();

    const params = QueryString.parse(decodeComma(location.search), { arrayFormat: 'comma' });
    const paramsWithoutFilters = getParamsWithoutFilters(params);
    const baseParams = { ...entityQueryParams, ...paramsWithoutFilters };
    const query: string = params?.query as string;
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;
    const selectedSortOption = useSelectedSortOption();
    const sortInput = useSortInput(selectedSortOption);
    const entityRegistry = useEntityRegistryV2();

    const searchableEntityTypes = entityRegistry.getSearchEntityTypes();

    const embeddedListSearchEntityTypes = [
        ...searchableEntityTypes,
        ...EXTRA_EMBEDDED_LIST_SEARCH_ENTITY_TYPES_TO_SUPPLEMENT_SEARCHABLE_ENTITY_TYPES,
    ];

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
            entityTypes={embeddedListSearchEntityTypes}
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
            useGetSearchCountResult={useGetSearchCountResult}
            shouldRefetch={shouldRefetch}
            resetShouldRefetch={resetShouldRefetch}
            applyView={applyView}
            showFilterBar={showFilterBar}
            sort={sortInput?.sortCriterion}
        />
    );
};
