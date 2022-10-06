import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { ApolloError } from '@apollo/client';
import { EntityType, FacetFilterInput } from '../../../../../../types.generated';
import { ENTITY_FILTER_NAME, UnionType } from '../../../../../search/utils/constants';
import { SearchCfg } from '../../../../../../conf';
import { EmbeddedListSearchResults } from './EmbeddedListSearchResults';
import EmbeddedListSearchHeader from './EmbeddedListSearchHeader';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { GetSearchResultsParams, SearchResultsInterface } from './types';
import { isListSubset } from '../../../utils';
import { EntityAndType } from '../../../types';
import { Message } from '../../../../../shared/Message';
import { generateOrFilters } from '../../../../../search/utils/generateOrFilters';

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

type Props = {
    query: string;
    page: number;
    unionType: UnionType;
    filters: FacetFilterInput[];
    onChangeQuery: (query) => void;
    onChangeFilters: (filters) => void;
    onChangePage: (page) => void;
    onChangeUnionType: (unionType: UnionType) => void;
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

export const EmbeddedListSearch = ({
    query,
    filters,
    page,
    unionType,
    onChangeQuery,
    onChangeFilters,
    onChangePage,
    onChangeUnionType,
    emptySearchQuery,
    fixedFilter,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    searchBarStyle,
    searchBarInputStyle,
    useGetSearchResults = useWrappedSearchResults,
}: Props) => {
    // Adjust query based on props
    const finalQuery: string = addFixedQuery(query as string, fixedQuery as string, emptySearchQuery as string);

    // Adjust filters based on props
    const filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    const finalFilters = (fixedFilter && [...filtersWithoutEntities, fixedFilter]) || filtersWithoutEntities;
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => filter.values.map((value) => value?.toUpperCase() as EntityType));

    const [showFilters, setShowFilters] = useState(defaultShowFilters || false);
    const [isSelectMode, setIsSelectMode] = useState(false);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const { refetch: refetchForDownload } = useGetSearchResults({
        variables: {
            input: {
                types: entityFilters,
                query: finalQuery,
                start: (page - 1) * SearchCfg.RESULTS_PER_PAGE,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: [],
                orFilters: generateOrFilters(unionType, filtersWithoutEntities),
            },
        },
        skip: true,
    });

    const callSearchOnVariables = (variables: GetSearchResultsParams['variables']) => {
        return refetchForDownload(variables);
    };

    const { data, loading, error, refetch } = useGetSearchResults({
        variables: {
            input: {
                types: entityFilters,
                query: finalQuery,
                start: (page - 1) * numResultsPerPage,
                count: numResultsPerPage,
                filters: [],
                orFilters: generateOrFilters(unionType, filtersWithoutEntities),
            },
        },
    });

    const searchResultEntities =
        data?.searchResults?.map((result) => ({ urn: result.entity.urn, type: result.entity.type })) || [];
    const searchResultUrns = searchResultEntities.map((entity) => entity.urn);
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

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
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode]);

    useEffect(() => {
        if (defaultFilters) {
            onChangeFilters(defaultFilters);
        }
        // only want to run once on page load
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // Filter out the persistent filter values
    const filteredFilters = data?.facets?.filter((facet) => facet.field !== fixedFilter?.field) || [];

    return (
        <Container>
            {error && <Message type="error" content="Failed to load results! An unexpected error occurred." />}
            <EmbeddedListSearchHeader
                onSearch={(q) => onChangeQuery(addFixedQuery(q, fixedQuery as string, emptySearchQuery as string))}
                placeholderText={placeholderText}
                onToggleFilters={onToggleFilters}
                callSearchOnVariables={callSearchOnVariables}
                entityFilters={entityFilters}
                filters={finalFilters}
                query={finalQuery}
                isSelectMode={isSelectMode}
                isSelectAll={selectedEntities.length > 0 && isListSubset(searchResultUrns, selectedEntityUrns)}
                setIsSelectMode={setIsSelectMode}
                selectedEntities={selectedEntities}
                onChangeSelectAll={onChangeSelectAll}
                refetch={refetch as any}
                searchBarStyle={searchBarStyle}
                searchBarInputStyle={searchBarInputStyle}
            />
            <EmbeddedListSearchResults
                unionType={unionType}
                loading={loading}
                searchResponse={data}
                filters={filteredFilters}
                selectedFilters={filters}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                onChangeUnionType={onChangeUnionType}
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
