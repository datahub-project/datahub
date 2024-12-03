import { Select, Tag } from 'antd';
import { Tooltip } from '@components';
import React, { useEffect, useState } from 'react';
import { useGetEntitiesLazyQuery } from '../../../../graphql/entity.generated';
import { useGetSearchResultsForMultipleLazyQuery } from '../../../../graphql/search.generated';
import { Entity, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntitySearchInputResult } from './EntitySearchInputResult';

type Props = {
    selectedUrns: string[];
    entityTypes: EntityType[];
    placeholder?: string;
    mode?: 'multiple' | 'single';
    style?: any;
    tagStyle?: React.CSSProperties;
    optionStyle?: React.CSSProperties;
    onChangeSelectedUrns: (newUrns: string[]) => void;
};

const addToCache = (cache: Map<string, Entity>, entity: Entity) => {
    cache.set(entity.urn, entity);
    return cache;
};

const buildCache = (entities: Entity[]) => {
    const cache = new Map();
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
 * TODO: Ideally an initial entity cache could be injected into the component when it's created.
 */
export const EntitySearchInput = ({
    selectedUrns,
    entityTypes,
    placeholder,
    style,
    tagStyle,
    optionStyle,
    mode,
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
            setEntityCache(buildCache(entities));
        }
    }, [resolvedEntitiesData]);

    /**
     * Response to user typing with search.
     */
    const [searchResources, { data: resourcesSearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const searchResults = resourcesSearchData?.searchAcrossEntities?.searchResults || [];
    const searchResultEntities = searchResults.map((result) => result.entity) as Entity[];
    const urnToSearchResultEntity = new Map();
    searchResults.forEach((result) => {
        urnToSearchResultEntity[result.entity.urn] = {
            urn: result.entity.urn,
            type: result.entity.type,
            displayName: entityRegistry.getDisplayName(result.entity.type, result.entity),
        };
    });

    const onSelect = (newUrn) => {
        const newEntity = searchResultEntities.find((entity) => entity.urn === newUrn);
        if (newEntity) {
            setEntityCache(addToCache(entityCache, newEntity));
            if (mode === 'single') {
                onChangeSelectedUrns([newUrn]);
            } else {
                const newUrns = [...selectedUrns, newUrn];
                onChangeSelectedUrns(newUrns);
            }
        }
    };

    const onDeselect = (urn) => {
        if (mode === 'single') {
            onChangeSelectedUrns([]);
        } else {
            onChangeSelectedUrns(selectedUrns.filter((u) => u !== urn));
        }
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

    return (
        <Select
            value={selectedUrns}
            mode="multiple"
            style={style}
            filterOption={false}
            placeholder={placeholder || 'Search for entities...'}
            onSelect={onSelect}
            onDeselect={onDeselect}
            onSearch={onSearch}
            data-testid="entity-search-input"
            tagRender={(tagProps) => {
                let displayName = tagProps.value;
                if (entityCache.has(tagProps.value as string)) {
                    const entity = entityCache.get(tagProps.value as string) as Entity;
                    displayName = entityRegistry.getDisplayName(entity?.type, entity) || displayName;
                }
                return (
                    <Tag closable={tagProps.closable} onClose={tagProps.onClose} style={tagStyle}>
                        <Tooltip title={displayName}>{displayName}</Tooltip>
                    </Tag>
                );
            }}
        >
            {searchResults?.map((result) => (
                <Select.Option
                    value={result.entity.urn}
                    style={optionStyle}
                    data-testid={`${result.entity.urn}-entity-search-input-result`}
                >
                    <EntitySearchInputResult entity={result.entity} />
                </Select.Option>
            ))}
        </Select>
    );
};
