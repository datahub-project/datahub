import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleForSearchBarLazyQuery,
} from '@src/graphql/search.generated';
import { AndFilterInput, Entity, EntityType, FacetMetadata } from '@src/types.generated';

type UpdateDataFunction = (
    query: string,
    orFilters: AndFilterInput[],
    types: EntityType[],
    viewUrn: string | undefined,
) => void;

type APIResponse = {
    updateData: UpdateDataFunction;
    facets?: FacetMetadata[];
    entities?: Entity[];
    loading?: boolean;
};

const useAutocompleteAPI = (): APIResponse => {
    const [entities, setEntities] = useState<Entity[] | undefined>();
    const [facets, setFacets] = useState<FacetMetadata[] | undefined>();
    const [getAutoCompleteMultipleResults, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], types: EntityType[], viewUrn: string | undefined) => {
            if (query.length === 0) {
                setEntities(undefined);
                setFacets(undefined);
            } else {
                getAutoCompleteMultipleResults({
                    variables: {
                        input: {
                            query,
                            orFilters,
                            types,
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

    const [getSearchResultsForMultiple, { data, loading }] = useGetSearchResultsForMultipleForSearchBarLazyQuery();

    // TODO:: pass types and viewUrn
    const updateData = useCallback(
        (query: string, orFilters: AndFilterInput[], types: EntityType[], viewUrn: string | undefined) => {
            // SearchAPI supports queries with 3 or longer characters
            if (query.length < 3) {
                setEntities(undefined);
                setFacets(undefined);
            } else {
                getSearchResultsForMultiple({
                    variables: {
                        input: {
                            query,
                            viewUrn,
                            orFilters,
                            count: 20,
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

// TODO:: add option for old search bar to skip query
export const useSearchBarData = (
    query: string,
    appliedFilters: FieldToAppliedFieldFiltersMap | undefined,
    searchAPIVariant: 'searchAcrossEntitiesAPI' | 'autocompleteAPI' | undefined,
) => {
    const [debouncedQuery, setDebouncedQuery] = useState<string>('');
    const autocompleteAPI = useAutocompleteAPI();
    const searchAPI = useSearchAPI();

    const api = useMemo(() => {
        switch (searchAPIVariant) {
            case 'searchAcrossEntitiesAPI':
                return searchAPI;
            case 'autocompleteAPI':
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

    useDebounce(
        () => {
            const convertedFilters = convertFiltersMapToFilters(appliedFilters);
            const orFilters = generateOrFilters(UnionType.AND, convertedFilters);

            updateData(debouncedQuery, orFilters, [], undefined);
        },
        300,
        [updateData, debouncedQuery, appliedFilters],
    );

    return { entities, facets, loading };
};
