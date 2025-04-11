import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleTrimmedLazyQuery,
} from '@src/graphql/search.generated';
import { AndFilterInput, Entity, FacetMetadata, SearchBarApi } from '@src/types.generated';

type UpdateDataFunction = (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => void;

type APIResponse = {
    updateData: UpdateDataFunction;
    facets?: FacetMetadata[];
    entities?: Entity[];
    loading?: boolean;
};

export type SearchResponse = {
    facets?: FacetMetadata[];
    entities?: Entity[];
    loading?: boolean;
    searchAPIVariant?: SearchBarApi;
};

const SEARCH_API_RESPONSE_MAX_ITEMS = 20;

const useAutocompleteAPI = (): APIResponse => {
    const [entities, setEntities] = useState<Entity[] | undefined>();
    const [facets, setFacets] = useState<FacetMetadata[] | undefined>();
    const [getAutoCompleteMultipleResults, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => {
            if (query.length === 0) {
                setEntities(undefined);
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
            setEntities(data?.autoCompleteForMultiple?.suggestions?.flatMap((Suggestion) => Suggestion.entities) || []);
            setFacets(undefined);
        }
    }, [data, loading]);

    return { updateData, entities, facets, loading };
};

const useSearchAPI = (): APIResponse => {
    const [entities, setEntities] = useState<Entity[] | undefined>();
    const [facets, setFacets] = useState<FacetMetadata[] | undefined>();

    const [getSearchResultsForMultiple, { data, loading }] = useGetSearchResultsForMultipleTrimmedLazyQuery();

    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], viewUrn: string | undefined | null) => {
            // SearchAPI supports queries with 3 or longer characters
            if (query.length < 3) {
                setEntities(undefined);
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
                        },
                    },
                });
            }
        },
        [getSearchResultsForMultiple],
    );

    useEffect(() => {
        if (!loading) {
            setEntities(data?.searchAcrossEntities?.searchResults?.map((searchResult) => searchResult.entity) || []);
            setFacets(data?.searchAcrossEntities?.facets || []);
        }
    }, [data, loading]);

    return { updateData, entities, facets, loading };
};

export const useSearchBarData = (
    query: string,
    appliedFilters: FieldToAppliedFieldFiltersMap | undefined,
    viewUrn: string | undefined | null,
    searchAPIVariant: SearchBarApi | undefined,
    enabled: boolean,
): SearchResponse => {
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

    useDebounce(() => setDebouncedQuery(query), 300, [query]);

    const updateData = useMemo(() => api.updateData, [api.updateData]);
    const entities = useMemo(() => api.entities, [api.entities]);
    const facets = useMemo(() => api.facets, [api.facets]);
    const loading = useMemo(() => api.loading, [api.loading]);

    useEffect(() => {
        if (enabled) {
            const convertedFilters = convertFiltersMapToFilters(appliedFilters);
            const orFilters = generateOrFilters(UnionType.AND, convertedFilters);

            updateData(debouncedQuery, orFilters, viewUrn);
        }
    }, [updateData, debouncedQuery, appliedFilters, viewUrn, enabled]);

    return { entities, facets, loading, searchAPIVariant };
};
