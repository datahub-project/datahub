import { Select, SelectOption } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { mergeArraysOfObjects } from '@app/utils/arrayUtils';

import { useGetEntitiesLazyQuery } from '@graphql/entity.generated';
import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const StyledOptionWrapper = styled.div`
    width: 100%;
`;

type Props = {
    selectedUrns: string[];
    entityTypes: EntityType[];
    mode?: 'multiple' | 'single';
    onChangeSelectedUrns: (newUrns: string[]) => void;
    label?: string;
    placeholder?: string;
};

const addManyToCache = (cache: Map<string, Entity>, entities: Entity[]) => {
    entities.forEach((entity) => cache.set(entity.urn, entity));
    return cache;
};

const isResolutionRequired = (urns: string[], cache: Map<string, Entity>) => {
    const uncachedUrns = urns.filter((urn) => !cache.has(urn));
    return uncachedUrns.length > 0;
};

/**
 * This component allows you to search and select entities. It will handle everything, including
 * resolving the entities to their display name when required.
 *
 * FYI: redesigned version of this component -  src/app/entityV2/shared/EntitySearchInput/EntitySearchInput.tsx
 */
export const EntitySearchValueInput = ({
    selectedUrns,
    entityTypes,
    mode,
    label,
    placeholder,
    onChangeSelectedUrns,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();
    useEffect(() => {
        if (isResolutionRequired(selectedUrns, entityCache)) {
            // Resolve urns to their full entities
            getEntities({ variables: { urns: selectedUrns } });
        }
    }, [selectedUrns, entityCache, getEntities]);

    /**
     * If some entities need to be resolved, simply build the cache from them.
     * This should only happen once at component bootstrap. Typically
     * all URNs will be missing from the cache.
     */
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache((cache) => addManyToCache(cache, entities));
        }
    }, [resolvedEntitiesData]);

    /**
     * Response to user typing with search.
     */
    const [searchResources, { data: resourcesSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const searchResults = useMemo(
        () => resourcesSearchData?.searchAcrossEntities?.searchResults || [],
        [resourcesSearchData],
    );

    useEffect(
        () =>
            setEntityCache((cache) =>
                addManyToCache(
                    cache,
                    searchResults.map((result) => result.entity),
                ),
            ),
        [searchResults],
    );

    const onUpdate = (updatedUrns: string[]) => {
        onChangeSelectedUrns(updatedUrns);
    };

    const onSearch = (text: string) => {
        searchResources({
            variables: {
                input: {
                    types: entityTypes,
                    query: text,
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    const options: SelectOption[] = useMemo(() => {
        const selectedEntities = selectedUrns
            .map((urn) => entityCache.get(urn))
            .filter((entity): entity is Entity => !!entity);
        const searchedEntities: Entity[] = searchResults.map((result) => result.entity);

        const mergedEntities = mergeArraysOfObjects(searchedEntities, selectedEntities, (item) => item.urn);

        const selectedSet = new Set(selectedUrns);
        return mergedEntities
            .map((entity) => ({
                value: entity.urn,
                label: entityRegistry.getDisplayName(entity.type, entity),
            }))
            .sort((a, b) => {
                const aSelected = selectedSet.has(a.value) ? 0 : 1;
                const bSelected = selectedSet.has(b.value) ? 0 : 1;
                return aSelected - bSelected;
            });
    }, [searchResults, entityCache, selectedUrns, entityRegistry]);

    const customOptionRenderer = useCallback(
        (option: SelectOption) => {
            const entity = entityCache.get(option.value);

            if (entity) {
                return (
                    <StyledOptionWrapper>
                        <AutoCompleteEntityItem entity={entity} variant="select" />
                    </StyledOptionWrapper>
                );
            }
            return <span>{option.label}</span>;
        },
        [entityCache],
    );

    /**
     * Issue a star search on component mount to provide a default set of results.
     */
    useEffect(() => {
        searchResources({
            variables: {
                input: {
                    types: entityTypes,
                    query: '*',
                    start: 0,
                    count: 10,
                },
            },
        });
    }, [entityTypes, searchResources]);

    const isMultiSelect = mode === 'multiple';

    return (
        <Select
            options={options}
            values={selectedUrns}
            isMultiSelect={isMultiSelect}
            onUpdate={onUpdate}
            onSearchChange={onSearch}
            placeholder={placeholder || 'Select a value...'}
            data-testid="entity-search-input"
            selectLabelProps={
                isMultiSelect && selectedUrns.length > 0
                    ? {
                          variant: 'labeled',
                          label: label ?? 'Items',
                      }
                    : undefined
            }
            showClear
            width="full"
            renderCustomOptionText={customOptionRenderer}
            showSearch
        />
    );
};
