import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    useGetAutoCompleteResultsLazyQuery,
    useGetEntitySearchResultsAutoCompleteFieldsLazyQuery,
} from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const SEARCH_LIMIT = 10;
const DATA_PRODUCT_SEARCH_DEBOUNCE_MS = 300;

interface Props {
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
}

function normalizeParentSearchQuery(query: string): string {
    return query.trim() || '*';
}

export default function useParentSelector({ entityType, entityData, selectedParentUrn, setSelectedParentUrn }: Props) {
    const [selectedParentName, setSelectedParentName] = useState<string>();
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('*');
    const entityRegistry = useEntityRegistry();
    const usesSearchAcross = entityType === EntityType.DataProduct;

    const [getAutoCompleteResults, { data: autoCompleteResultsValue, loading: autoCompleteResultsLoading }] =
        useGetAutoCompleteResultsLazyQuery();
    const [searchAcrossEntities, { data: searchAcrossResultsValue, loading: searchAcrossResultsLoading }] =
        useGetEntitySearchResultsAutoCompleteFieldsLazyQuery();

    useDebounce(
        () => setDebouncedSearchQuery(normalizeParentSearchQuery(searchQuery)),
        usesSearchAcross ? DATA_PRODUCT_SEARCH_DEBOUNCE_MS : 0,
        [searchQuery, usesSearchAcross],
    );

    const fetchResults = useCallback(
        (query: string) => {
            const normalizedQuery = normalizeParentSearchQuery(query);

            if (usesSearchAcross) {
                searchAcrossEntities({
                    variables: {
                        input: {
                            types: [entityType],
                            query: normalizedQuery,
                            start: 0,
                            count: SEARCH_LIMIT,
                        },
                    },
                });
                return;
            }

            getAutoCompleteResults({
                variables: {
                    input: {
                        type: entityType,
                        query: normalizedQuery,
                        limit: SEARCH_LIMIT,
                    },
                },
            });
        },
        [entityType, getAutoCompleteResults, searchAcrossEntities, usesSearchAcross],
    );

    // DataProduct search is debounced via effect; Domain/other types fetch directly in handleSearch.
    useEffect(() => {
        if (!usesSearchAcross) {
            return;
        }
        fetchResults(debouncedSearchQuery);
    }, [debouncedSearchQuery, fetchResults, usesSearchAcross]);

    useEffect(() => {
        if (entityData && selectedParentUrn === entityData.urn) {
            const displayName = entityRegistry.getDisplayName(entityType, entityData);
            setSelectedParentName(displayName);
        }
    }, [entityData, entityRegistry, selectedParentUrn, entityData?.urn, entityType]);

    const searchResults = useMemo((): Entity[] => {
        if (usesSearchAcross) {
            // Hide previous matches while the typed query is still debouncing.
            if (normalizeParentSearchQuery(searchQuery) !== debouncedSearchQuery) {
                return [];
            }
            return (
                searchAcrossResultsValue?.searchAcrossEntities?.searchResults
                    ?.map((result) => result.entity)
                    .filter((entity): entity is Entity => !!entity) ?? []
            );
        }

        return autoCompleteResultsValue?.autoComplete?.entities ?? [];
    }, [autoCompleteResultsValue, debouncedSearchQuery, searchAcrossResultsValue, searchQuery, usesSearchAcross]);

    function handleSearch(text: string) {
        setSearchQuery(text);
        if (!usesSearchAcross) {
            fetchResults(text);
        }
    }

    function onSelectParent(parentUrn: string) {
        const selectedParent = searchResults.find((result) => result.urn === parentUrn);
        if (selectedParent) {
            setSelectedParentUrn(parentUrn);
            const displayName = entityRegistry.getDisplayName(selectedParent.type, selectedParent);
            setSelectedParentName(displayName);
        }
    }

    function clearSelectedParent() {
        setSelectedParentUrn('');
        setSelectedParentName(undefined);
        setSearchQuery('');
        if (usesSearchAcross) {
            setDebouncedSearchQuery('*');
        }
    }

    function selectParentFromBrowser(urn: string, displayName: string) {
        setIsFocusedOnInput(false);
        setSelectedParentUrn(urn);
        setSelectedParentName(displayName);
    }

    return {
        searchQuery,
        searchResults,
        isFocusedOnInput,
        selectedParentName,
        onSelectParent,
        handleSearch,
        setIsFocusedOnInput,
        selectParentFromBrowser,
        clearSelectedParent,
        autoCompleteResultsLoading: usesSearchAcross ? searchAcrossResultsLoading : autoCompleteResultsLoading,
    };
}
