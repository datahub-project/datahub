import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { ApolloError } from '@apollo/client';
import {
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    SearchAcrossEntitiesInput,
} from '../../../../../../types.generated';
import { DEGREE_FILTER_NAME, UnionType } from '../../../../../search/utils/constants';
import { SearchCfg } from '../../../../../../conf';
import { EmbeddedListSearchResults } from './EmbeddedListSearchResults';
import EmbeddedListSearchHeader from './EmbeddedListSearchHeader';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { FilterSet, GetSearchResultsParams, SearchResultsInterface } from './types';
import { isListSubset } from '../../../utils';
import { EntityAndType } from '../../../types';
import { Message } from '../../../../../shared/Message';
import { generateOrFilters } from '../../../../../search/utils/generateOrFilters';
import { mergeFilterSets } from '../../../../../search/utils/filterUtils';
import { useDownloadScrollAcrossEntitiesSearchResults } from '../../../../../search/utils/useDownloadScrollAcrossEntitiesSearchResults';
import {
    DownloadSearchResultsParams,
    DownloadSearchResultsInput,
    DownloadSearchResults,
} from '../../../../../search/utils/types';
import { useEntityContext } from '../../../EntityContext';
import { EntityActionProps } from './EntitySearchResults';
import { useUserContext } from '../../../../../context/useUserContext';
import analytics, { EventType } from '../../../../../analytics';

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

// Simply remove the fields that were marked as fixed from the facets that the server
// responds.
export const removeFixedFiltersFromFacets = (fixedFilters: FilterSet, facets: FacetMetadata[]) => {
    const fixedFields = fixedFilters.filters.map((filter) => filter.field);
    return facets.filter((facet) => !fixedFields.includes(facet.field));
};

type Props = {
    query: string;
    entityTypes?: EntityType[];
    page: number;
    unionType: UnionType;
    filters: FacetFilterInput[];
    onChangeQuery: (query) => void;
    onChangeFilters: (filters) => void;
    onChangePage: (page) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onTotalChanged?: (newTotal: number) => void;
    emptySearchQuery?: string | null;
    fixedFilters?: FilterSet;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
    entityAction?: React.FC<EntityActionProps>;
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
};

export const EmbeddedListSearch = ({
    query,
    entityTypes,
    filters,
    page,
    unionType,
    onChangeQuery,
    onChangeFilters,
    onChangePage,
    onChangeUnionType,
    onTotalChanged,
    emptySearchQuery,
    fixedFilters,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    searchBarStyle,
    searchBarInputStyle,
    entityAction,
    skipCache,
    useGetSearchResults = useWrappedSearchResults,
    useGetDownloadSearchResults = useDownloadScrollAcrossEntitiesSearchResults,
    shouldRefetch,
    resetShouldRefetch,
    applyView = false,
}: Props) => {
    const { shouldRefetchEmbeddedListSearch, setShouldRefetchEmbeddedListSearch } = useEntityContext();
    // Adjust query based on props
    const finalQuery: string = addFixedQuery(query as string, fixedQuery as string, emptySearchQuery as string);

    const baseFilters = {
        unionType,
        filters,
    };

    const finalFilters =
        (fixedFilters && mergeFilterSets(fixedFilters, baseFilters)) || generateOrFilters(unionType, filters);

    const [showFilters, setShowFilters] = useState(defaultShowFilters || false);
    const [isSelectMode, setIsSelectMode] = useState(false);
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    // This hook is simply used to generate a refetch callback that the DownloadAsCsv component can use to
    // download the correct results given the current context.
    // TODO: Use the loading indicator to log a message to the user should download to CSV fail.
    // TODO: Revisit this pattern -- what can we push down?
    const { refetch: refetchForDownload } = useGetDownloadSearchResults({
        variables: {
            input: {
                types: entityTypes || [],
                query,
                count: SearchCfg.RESULTS_PER_PAGE,
                orFilters: generateOrFilters(unionType, filters),
                scrollId: null,
            },
        },
        skip: true,
    });

    const userContext = useUserContext();
    const selectedViewUrn = userContext.localState?.selectedViewUrn;

    let searchInput: SearchAcrossEntitiesInput = {
        types: entityTypes || [],
        query: finalQuery,
        start: (page - 1) * numResultsPerPage,
        count: numResultsPerPage,
        orFilters: finalFilters,
        viewUrn: applyView ? selectedViewUrn : undefined,
    };
    if (skipCache) {
        searchInput = { ...searchInput, searchFlags: { skipCache: true } };
    }

    const { data, loading, error, refetch } = useGetSearchResults({
        variables: {
            input: searchInput,
        },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (shouldRefetch && resetShouldRefetch) {
            refetch({
                input: searchInput,
            });
            resetShouldRefetch();
        }
    });

    useEffect(() => {
        if (shouldRefetchEmbeddedListSearch) {
            refetch({
                input: searchInput,
            });
            setShouldRefetchEmbeddedListSearch?.(false);
        }
    });

    useEffect(() => {
        if (data?.total !== undefined && onTotalChanged) {
            onTotalChanged(data?.total);
        }
    }, [data?.total, onTotalChanged]);

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
        if (defaultFilters && filters.length === 0) {
            onChangeFilters(defaultFilters);
        }
        // only want to run once on page load
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    /**
     * Compute the final Facet fields that we show in the left hand search Filters (aggregation).
     *
     * Do this by filtering out any fields that are included in the fixed filters.
     */
    const finalFacets =
        (fixedFilters && removeFixedFiltersFromFacets(fixedFilters, data?.facets || [])) || data?.facets;

    // used for logging impact anlaysis events
    const degreeFilter = filters.find((filter) => filter.field === DEGREE_FILTER_NAME);

    // we already have some lineage logging through Tab events, but this adds additional context, particularly degree
    if (!loading && (degreeFilter?.values?.length || 0) > 0) {
        analytics.event({
            type: EventType.SearchAcrossLineageResultsViewEvent,
            query,
            page,
            total: data?.total || 0,
            maxDegree: degreeFilter?.values?.sort()?.reverse()[0] || '1',
        });
    }

    return (
        <Container>
            {error && <Message type="error" content="Failed to load results! An unexpected error occurred." />}
            <EmbeddedListSearchHeader
                onSearch={(q) => onChangeQuery(addFixedQuery(q, fixedQuery as string, emptySearchQuery as string))}
                placeholderText={placeholderText}
                onToggleFilters={onToggleFilters}
                downloadSearchResults={(input) => refetchForDownload(input)}
                filters={finalFilters}
                query={finalQuery}
                isSelectMode={isSelectMode}
                isSelectAll={selectedEntities.length > 0 && isListSubset(searchResultUrns, selectedEntityUrns)}
                setIsSelectMode={setIsSelectMode}
                selectedEntities={selectedEntities}
                onChangeSelectAll={onChangeSelectAll}
                refetch={() => refetch({ input: searchInput })}
                searchBarStyle={searchBarStyle}
                searchBarInputStyle={searchBarInputStyle}
            />
            <EmbeddedListSearchResults
                unionType={unionType}
                loading={loading}
                searchResponse={data}
                filters={finalFacets}
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
                entityAction={entityAction}
                applyView={applyView}
            />
        </Container>
    );
};
