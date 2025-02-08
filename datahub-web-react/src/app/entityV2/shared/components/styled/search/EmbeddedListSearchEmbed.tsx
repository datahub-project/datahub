import React, { useState } from 'react';
import { ApolloError } from '@apollo/client';
import useSortInput from '@src/app/searchV2/sorting/useSortInput';
import { useSelectedSortOption } from '@src/app/search/context/SearchContext';
import { FacetFilterInput } from '../../../../../../types.generated';
import { FilterSet, GetSearchResultsParams, SearchResultsInterface } from './types';
import { EmbeddedListSearch } from './EmbeddedListSearch';
import { UnionType } from '../../../../../search/utils/constants';
import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '../../../../../search/utils/types';

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

export const EmbeddedListSearchEmbed = ({
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
    // Component state
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState(1);
    const [unionType, setUnionType] = useState(UnionType.AND);

    const [filters, setFilters] = useState<Array<FacetFilterInput>>([]);
    const selectedSortOption = useSelectedSortOption();
    const sortInput = useSortInput(selectedSortOption);

    const onChangeQuery = (q: string) => {
        setQuery(q);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        setFilters(newFilters);
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    return (
        <EmbeddedListSearch
            query={query}
            page={page}
            unionType={unionType}
            filters={filters}
            onChangeFilters={onChangeFilters}
            onChangeQuery={onChangeQuery}
            onChangePage={onChangePage}
            onChangeUnionType={setUnionType}
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
            useGetSearchCountResult={useGetSearchCountResult}
            resetShouldRefetch={resetShouldRefetch}
            applyView={applyView}
            showFilterBar={showFilterBar}
            sort={sortInput?.sortCriterion}
        />
    );
};
