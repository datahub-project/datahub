import { isEqual } from 'lodash';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import useSelectedView from '@app/searchV2/searchBarV2/hooks/useSelectedView';
import { EntityWithMatchedFields } from '@app/searchV2/utils/combineSiblingsInEntitiesWithMatchedFields';
import { MIN_CHARACTER_COUNT_FOR_SEARCH, UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import usePrevious from '@app/shared/usePrevious';
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

export type SearchResponse = {
    facets?: FacetMetadata[];
    entitiesWithMatchedFields?: EntityWithMatchedFields[];
    loading?: boolean;
    searchAPIVariant?: SearchBarApi;
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
                        },
                    },
                });
            }
        },
        [getSearchResultsForMultiple],
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

    const inputs = useMemo(
        () => ({ debouncedQuery, orFilters, selectedView }),
        [debouncedQuery, orFilters, selectedView],
    );
    const previousInputs = usePrevious(inputs);
    useEffect(() => {
        if (!isEqual(inputs, previousInputs)) {
            updateData(debouncedQuery, orFilters, selectedView);
        }
    }, [updateData, debouncedQuery, orFilters, selectedView, inputs, previousInputs]);

    return { entitiesWithMatchedFields, facets, loading, searchAPIVariant };
};
