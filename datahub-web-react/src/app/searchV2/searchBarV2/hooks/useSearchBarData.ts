import isEqual from 'lodash/isEqual';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useDebounce } from 'react-use';

import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import useSelectedView from '@app/searchV2/searchBarV2/hooks/useSelectedView';
import { EntityWithMatchedFields } from '@app/searchV2/utils/combineSiblingsInEntitiesWithMatchedFields';
import { MIN_CHARACTER_COUNT_FOR_SEARCH, UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { useAppConfig } from '@app/useAppConfig';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleTrimmedLazyQuery,
} from '@src/graphql/search.generated';
import { AndFilterInput, FacetMetadata, SearchBarApi } from '@src/types.generated';

type UpdateDataFunction = (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => void;

type APIResponse = {
    updateData: UpdateDataFunction;
    facets?: FacetMetadata[];
    entitiesWithMatchedFields?: EntityWithMatchedFields[];
    loading?: boolean;
};

type SearchResponse = {
    facets?: FacetMetadata[];
    entitiesWithMatchedFields?: EntityWithMatchedFields[];
    loading?: boolean;
    searchAPIVariant?: SearchBarApi;
};

export type SearchBarDataOptions = {
    /** Input focused or the typeahead dropdown open. */
    isActive: boolean;
    /** Query present when the search bar mounted, usually the URL query. */
    initialQuery: string;
    /** True once the user has edited the query in this session. */
    hasUserTyped: boolean;
};

type SearchBarFetchInputs = {
    query: string;
    orFilters: AndFilterInput[];
    viewUrn: string | null | undefined;
};

const SEARCH_API_RESPONSE_MAX_ITEMS = 20;
const DEBOUNCE_MS = 300;

const useAutocompleteAPI = (): APIResponse => {
    const [entitiesWithMatchedFields, setEntitiesWithMatchedFields] = useState<EntityWithMatchedFields[] | undefined>();
    const [facets, setFacets] = useState<FacetMetadata[] | undefined>();
    const [getAutoCompleteMultipleResults, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => {
            if (query.length === 0) {
                setEntitiesWithMatchedFields(undefined);
                setFacets(undefined);
            } else {
                getAutoCompleteMultipleResults({
                    variables: {
                        input: {
                            query,
                            orFilters,
                            viewUrn,
                        },
                    },
                });
            }
        },
        [getAutoCompleteMultipleResults],
    );

    useEffect(() => {
        if (!loading) {
            setEntitiesWithMatchedFields(
                data?.autoCompleteForMultiple?.suggestions
                    ?.flatMap((suggestion) => suggestion.entities)
                    ?.map((entity) => ({ entity })) || [],
            );
            setFacets(undefined);
        }
    }, [data, loading]);

    return { updateData, entitiesWithMatchedFields, facets, loading };
};

const useSearchAPI = (): APIResponse => {
    const { config } = useAppConfig();

    const [entitiesWithMatchedFields, setEntitiesWithMatchedFields] = useState<EntityWithMatchedFields[] | undefined>();
    const [facets, setFacets] = useState<FacetMetadata[] | undefined>();

    const [getSearchResultsForMultiple, { data, loading }] = useGetSearchResultsForMultipleTrimmedLazyQuery();

    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => {
            // SearchAPI supports queries with 3 or more characters
            if (query.length < MIN_CHARACTER_COUNT_FOR_SEARCH) {
                setEntitiesWithMatchedFields(undefined);
                // set to empty array instead of undefined to forcibly control facets
                // FYI: undefined triggers requests to get facets. see `filtersV2/SearchFilters` for details
                setFacets([]);
            } else {
                getSearchResultsForMultiple({
                    variables: {
                        input: {
                            query,
                            viewUrn,
                            orFilters,
                            count: SEARCH_API_RESPONSE_MAX_ITEMS,
                            searchFlags: {
                                skipHighlighting: config?.searchFlagsConfig?.defaultSkipHighlighting || false,
                            },
                        },
                    },
                });
            }
        },
        [getSearchResultsForMultiple, config?.searchFlagsConfig?.defaultSkipHighlighting],
    );

    useEffect(() => {
        if (!loading) {
            setEntitiesWithMatchedFields(
                data?.searchAcrossEntities?.searchResults?.map((searchResult) => ({
                    entity: searchResult.entity,
                    matchedFields: searchResult.matchedFields,
                })) || [],
            );
            setFacets(data?.searchAcrossEntities?.facets || []);
        }
    }, [data, loading]);

    return { updateData, entitiesWithMatchedFields, facets, loading };
};

export const useSearchBarData = (
    query: string,
    appliedFilters: FieldToAppliedFieldFiltersMap | undefined,
    { isActive, initialQuery, hasUserTyped }: SearchBarDataOptions,
): SearchResponse => {
    const { selectedView } = useSelectedView();
    const appConfig = useAppConfig();
    const searchAPIVariant = appConfig.config.searchBarConfig.apiVariant;
    const [debouncedQuery, setDebouncedQuery] = useState<string>('');
    const autocompleteAPI = useAutocompleteAPI();
    const searchAPI = useSearchAPI();

    const api = useMemo(() => {
        switch (searchAPIVariant) {
            case SearchBarApi.SearchAcrossEntities:
                return searchAPI;
            case SearchBarApi.AutocompleteForMultiple:
                return autocompleteAPI;
            default:
                return autocompleteAPI;
        }
    }, [searchAPIVariant, autocompleteAPI, searchAPI]);

    useDebounce(() => setDebouncedQuery(query), DEBOUNCE_MS, [query]);

    const updateData = useMemo(() => api.updateData, [api.updateData]);
    const entitiesWithMatchedFields = useMemo(() => api.entitiesWithMatchedFields, [api.entitiesWithMatchedFields]);
    const facets = useMemo(() => api.facets, [api.facets]);
    const loading = useMemo(() => api.loading, [api.loading]);
    const convertedFilters = convertFiltersMapToFilters(appliedFilters);
    const orFilters = generateOrFilters(UnionType.AND, convertedFilters);

    // Filters applied when the bar mounted (typically from the URL). Later edits are user intent.
    const pageOpenFiltersRef = useRef<AndFilterInput[] | null>(null);
    if (pageOpenFiltersRef.current === null) {
        pageOpenFiltersRef.current = orFilters;
    }
    const pageOpenQueryRef = useRef(initialQuery);
    // Same baseline as filters: the view present when the bar mounted. A later change,
    // including one made while the input is blurred, fetches on the next focus.
    const pageOpenViewRef = useRef(selectedView);
    const lastFetchedRef = useRef<SearchBarFetchInputs | null>(null);

    useEffect(() => {
        if (!isActive) return;

        // Hold the request until the debounced query matches the current input.
        if (debouncedQuery !== query) return;

        const inputs: SearchBarFetchInputs = { query: debouncedQuery, orFilters, viewUrn: selectedView };
        const filtersChanged = !isEqual(orFilters, pageOpenFiltersRef.current);
        const viewChanged = !isEqual(selectedView, pageOpenViewRef.current);
        const queryDiffersFromPageOpen = debouncedQuery !== pageOpenQueryRef.current;
        const inputsChangedSinceFetch = lastFetchedRef.current !== null && !isEqual(inputs, lastFetchedRef.current);
        const shouldFetch =
            hasUserTyped || queryDiffersFromPageOpen || filtersChanged || viewChanged || inputsChangedSinceFetch;

        if (!shouldFetch || isEqual(inputs, lastFetchedRef.current)) return;

        lastFetchedRef.current = inputs;
        updateData(debouncedQuery, orFilters, selectedView);
    }, [debouncedQuery, hasUserTyped, isActive, orFilters, query, selectedView, updateData]);

    return { entitiesWithMatchedFields, facets, loading, searchAPIVariant };
};
