import React, { useState, useEffect } from 'react';
import * as QueryString from 'query-string';
import { useHistory, useLocation, useParams } from 'react-router';
import { message } from 'antd';
import styled from 'styled-components';
import { ApolloError } from '@apollo/client';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput } from '../../../../../../types.generated';
import useFilters from '../../../../../search/utils/useFilters';
import { ENTITY_FILTER_NAME } from '../../../../../search/utils/constants';
import { SearchCfg } from '../../../../../../conf';
import { navigateToEntitySearchUrl } from './navigateToEntitySearchUrl';
import { EmbeddedListSearchResults } from './EmbeddedListSearchResults';
import EmbeddedListSearchHeader from './EmbeddedListSearchHeader';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { GetSearchResultsParams, SearchResultsInterface } from './types';
import { useEntityQueryParams } from '../../../containers/profile/utils';
import { isListSubset } from '../../../utils';
import { EntityAndType } from '../../../types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow-y: hidden;
`;

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
// the addFixedQuery checks and generate the query as per params pass to embeddedListSearch
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

type SearchPageParams = {
    type?: string;
};

type Props = {
    emptySearchQuery?: string | null;
    fixedFilter?: FacetFilterInput | null;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    useGetSearchResults?: (params: GetSearchResultsParams) => {
        data: SearchResultsInterface | undefined | null;
        loading: boolean;
        error: ApolloError | undefined;
        refetch: (variables: GetSearchResultsParams['variables']) => Promise<SearchResultsInterface | undefined | null>;
    };
};

export const EmbeddedListSearch = ({
    emptySearchQuery,
    fixedFilter,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    useGetSearchResults = useWrappedSearchResults,
}: Props) => {
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();
    const baseParams = useEntityQueryParams();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = addFixedQuery(params?.query as string, fixedQuery as string, emptySearchQuery as string);
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

    const [showFilters, setShowFilters] = useState(defaultShowFilters || false);
    const [isSelectMode, setIsSelectMode] = useState(false);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const { refetch } = useGetSearchResults({
        variables: {
            input: {
                types: entityFilters,
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

    const { data, loading, error } = useGetSearchResults({
        variables: {
            input: {
                types: entityFilters,
                query,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                filters: finalFilters,
            },
        },
    });

    const searchResultEntities =
        data?.searchResults?.map((result) => ({ urn: result.entity.urn, type: result.entity.type })) || [];
    const searchResultUrns = searchResultEntities.map((entity) => entity.urn);
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

    const onSearch = (q: string) => {
        const finalQuery = addFixedQuery(q as string, fixedQuery as string, emptySearchQuery as string);
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
            type: activeType,
            query: finalQuery,
            page: 1,
            history,
        });
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToEntitySearchUrl({
            baseUrl: location.pathname,
            baseParams,
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
            baseParams,
            type: activeType,
            query,
            page: newPage,
            filters,
            history,
        });
    };

    const onToggleFilters = () => {
        setShowFilters(!showFilters);
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
        if (defaultFilters) {
            onChangeFilters(defaultFilters);
        }
        // only want to run once on page load
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode]);

    // Filter out the persistent filter values
    const filteredFilters = data?.facets?.filter((facet) => facet.field !== fixedFilter?.field) || [];

    return (
        <Container>
            {error && message.error(`Failed to complete search: ${error && error.message}`)}
            <EmbeddedListSearchHeader
                onSearch={onSearch}
                placeholderText={placeholderText}
                onToggleFilters={onToggleFilters}
                callSearchOnVariables={callSearchOnVariables}
                entityFilters={entityFilters}
                filters={finalFilters}
                query={query}
                isSelectMode={isSelectMode}
                isSelectAll={selectedEntities.length > 0 && isListSubset(searchResultUrns, selectedEntityUrns)}
                setIsSelectMode={setIsSelectMode}
                selectedEntities={selectedEntities}
                onChangeSelectAll={onChangeSelectAll}
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
                numResultsPerPage={numResultsPerPage}
                setNumResultsPerPage={setNumResultsPerPage}
                isSelectMode={isSelectMode}
                selectedEntities={selectedEntities}
                setSelectedEntities={setSelectedEntities}
            />
        </Container>
    );
};
