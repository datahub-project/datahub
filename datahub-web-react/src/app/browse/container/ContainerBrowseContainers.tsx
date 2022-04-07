import React, { useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { message } from 'antd';
import { ApolloError } from '@apollo/client';

import { ContainerBrowseSearchResults } from './ContainerBrowseSearchResults';
import { navigateToContainerBrowseUrl } from './navigateToContainerBrowseUrl';
import { SearchCfg, BrowseCfg } from '../../../conf';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { FacetFilterInput, EntityType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import EmbeddedListSearchHeader from '../../entity/shared/components/styled/search/EmbeddedListSearchHeader';
import { GetSearchResultsParams, SearchResultsInterface } from '../../entity/shared/components/styled/search/types';
import { ENTITY_FILTER_NAME, CONTAINER_FILTER_NAME } from '../../search/utils/constants';
import useFilters from '../../search/utils/useFilters';

type Filter = { field: string; value: string };

// this extracts the response from useGetSearchResultsForMultipleQuery into a common interface other search endpoints can also produce
function useWrappedSearchResults(params: GetSearchResultsParams) {
    const { data, loading, error, refetch } = useGetSearchResultsForMultipleQuery(params);
    return {
        data: data?.searchAcrossEntities,
        loading,
        error,
        refetch: (refetchParams: GetSearchResultsParams['variables']) =>
            refetch(refetchParams).then((res) => res.data.searchAcrossEntities),
    };
}

// the addFixedQuery checks and generate the query as per params pass
export const addFixedQuery = (baseQuery: string, fixedQuery: string, emptyQuery: string) => {
    let finalQuery = ``;
    if (baseQuery && fixedQuery) {
        finalQuery = baseQuery.includes(fixedQuery) ? `${baseQuery}` : `(*${baseQuery}*) AND (${fixedQuery})`;
    } else if (baseQuery) {
        finalQuery = `${baseQuery}`;
    } else if (fixedQuery) {
        finalQuery = `${fixedQuery}`;
    } else {
        return emptyQuery || '';
    }
    return finalQuery;
};

interface Props {
    fixedFilter?: FacetFilterInput | null;
    fixedQuery?: string | null;
    rootPath: string;
    entityRegistry: EntityRegistry;
    emptySearchQuery?: string | null;
    placeholderText?: string | null;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
        refetch: (variables: GetSearchResultsParams['variables']) => Promise<SearchResultsInterface | undefined | null>;
    };
    entityType: EntityType;
}

export const ContainerBrowseContainers = ({
    rootPath,
    fixedFilter,
    entityRegistry,
    fixedQuery,
    emptySearchQuery,
    placeholderText,
    useGetSearchResults = useWrappedSearchResults,
    entityType,
}: Props) => {
    const history = useHistory();
    const location = useLocation();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    let query: string = addFixedQuery(params?.query as string, fixedQuery as string, emptySearchQuery as string);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const filters: Array<FacetFilterInput> = useFilters(params);
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );

    const [showFilters, setShowFilters] = useState(false);
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .map((filter) => filter.value.toUpperCase() as EntityType);
    const path = rootPath.split('/');
    let finalFilters: Array<Filter> =
        (fixedFilter && [...filtersWithoutEntities, fixedFilter]) || filtersWithoutEntities;
    if (path.length > 4) {
        finalFilters = [...finalFilters, { field: CONTAINER_FILTER_NAME, value: path.slice(path.length - 1)[0] }];
        query = addFixedQuery(params?.query as string, '', emptySearchQuery as string);
    }
    const { refetch } = useGetSearchResults({
        variables: {
            input: {
                types: [entityType, entityRegistry.getTypeFromPathName(CONTAINER_FILTER_NAME)],
                query,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: finalFilters,
            },
        },
        skip: true,
    });

    const callSearchOnVariables = (variables: GetSearchResultsParams['variables']) => {
        return refetch(variables);
    };

    // Fetching the Meta Data Results
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [entityType, entityRegistry.getTypeFromPathName(CONTAINER_FILTER_NAME)],
                query,
                start: (page - 1) * BrowseCfg.RESULTS_PER_PAGE,
                count: BrowseCfg.RESULTS_PER_PAGE,
                filters: finalFilters,
            },
        },
    });

    const onSearch = (q: string) => {
        let finalQuery = q;
        if (q.trim().length === 0) {
            if (emptySearchQuery) {
                finalQuery = emptySearchQuery;
            } else {
                return;
            }
        }
        navigateToContainerBrowseUrl({
            baseUrl: location.pathname,
            query: finalQuery,
            page: 1,
            history,
        });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToContainerBrowseUrl({
            baseUrl: location.pathname,
            query,
            page: 1,
            filters: newFilters,
            history,
        });
    };

    const onChangePage = (newPage: number) => {
        navigateToContainerBrowseUrl({
            baseUrl: location.pathname,
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
    const filteredFilters =
        data?.searchAcrossEntities?.facets
            ?.filter((facet) => facet.field !== fixedFilter?.field)
            .filter((filter) => {
                return filter.field !== ENTITY_FILTER_NAME;
            }) || [];

    return (
        <>
            {error && message.error(`Failed to complete search: ${error && error.message}`)}
            <EmbeddedListSearchHeader
                onSearch={onSearch}
                placeholderText={placeholderText}
                onToggleFilters={toggleFilters}
                showDownloadCsvButton
                entityFilters={entityFilters}
                filters={finalFilters}
                callSearchOnVariables={callSearchOnVariables}
                query={query}
            />
            <ContainerBrowseSearchResults
                loading={loading}
                searchResponse={data?.searchAcrossEntities}
                filters={filteredFilters}
                selectedFilters={finalFilters}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                page={page}
                showFilters={showFilters}
                entityRegistry={entityRegistry}
                entityType={entityType}
                rootPath={rootPath}
            />
        </>
    );
};
