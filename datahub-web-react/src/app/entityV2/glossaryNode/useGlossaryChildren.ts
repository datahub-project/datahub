import { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';
import { useDebounce } from 'react-use';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { ENTITY_INDEX_FILTER_NAME } from '@src/app/search/utils/constants';
import { ENTITY_NAME_FIELD } from '@src/app/searchV2/context/constants';
import { useGetAutoCompleteMultipleResultsQuery, useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { Entity, EntityType, SortOrder } from '@src/types.generated';

export const GLOSSARY_CHILDREN_COUNT = 50;

function getGlossaryChildrenScrollInput(urn: string, scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
            orFilters: [{ and: [{ field: 'parentNode', values: [urn || ''] }] }],
            count: GLOSSARY_CHILDREN_COUNT,
            sortInput: {
                sortCriteria: [
                    { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                    { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                ],
            },
        },
    };
}

interface Props {
    entityUrn?: string;
    skip?: boolean;
}

export default function useGlossaryChildren({ entityUrn, skip }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { nodeToNewEntity, setNodeToNewEntity, setNodeToDeletedUrn, nodeToDeletedUrn } = useGlossaryEntityData();
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [query, setQuery] = useState<string>(''); // query to use in auto-complete. gets debounced
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [searchData, setSearchData] = useState<Entity[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [data, setData] = useState<Entity[]>([]);
    const { data: scrollData, loading } = useScrollAcrossEntitiesQuery({
        variables: {
            ...getGlossaryChildrenScrollInput(entityUrn || '', scrollId),
        },
        skip: !entityUrn || skip,
        notifyOnNetworkStatusChange: true,
    });
    const shouldDoAutoComplete = data.length >= GLOSSARY_CHILDREN_COUNT;

    // Handle initial data and updates from scroll
    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const newResults = scrollData.scrollAcrossEntities.searchResults
                .filter((r) => !dataUrnsSet.has(r.entity.urn))
                .map((r) => r.entity);

            if (newResults.length > 0) {
                setData((currData) => [...currData, ...newResults]);
                setDataUrnsSet((currSet) => {
                    const newSet = new Set(currSet);
                    newResults.forEach((r) => newSet.add(r.urn));
                    return newSet;
                });
            }
        }
    }, [scrollData, dataUrnsSet]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    useDebounce(() => setQuery(searchQuery), 250, [searchQuery]);

    const { loading: autoCompleteLoading } = useGetAutoCompleteMultipleResultsQuery({
        variables: {
            input: {
                query,
                types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                limit: 100,
                orFilters: [{ and: [{ field: 'parentNode', values: [entityUrn as string] }] }],
            },
        },
        skip: !query || !entityUrn || !shouldDoAutoComplete,
        onCompleted: (d) => {
            const results = d.autoCompleteForMultiple?.suggestions.flatMap((s) => s.entities);
            if (results) {
                setSearchData(results);
            }
        },
    });

    // update when new entity is added
    useEffect(() => {
        if (entityUrn && nodeToNewEntity[entityUrn] && !dataUrnsSet.has(nodeToNewEntity[entityUrn].urn)) {
            const newEntity = nodeToNewEntity[entityUrn];
            setData((currData) => [newEntity, ...currData]);
            setDataUrnsSet((currSet) => new Set([...currSet, newEntity.urn]));
            setNodeToNewEntity((prev) => {
                const newState = { ...prev };
                delete newState[entityUrn];
                return newState;
            });
        }
    }, [entityUrn, nodeToNewEntity, setNodeToNewEntity, dataUrnsSet]);

    // update when entity is removed
    useEffect(() => {
        if (entityUrn && nodeToDeletedUrn[entityUrn]) {
            const deletedUrn = nodeToDeletedUrn[entityUrn];
            setData((currData) => currData.filter((e) => e.urn !== deletedUrn));
            setNodeToDeletedUrn((prev) => {
                const newState = { ...prev };
                delete newState[entityUrn];
                return newState;
            });
        }
    }, [entityUrn, nodeToDeletedUrn, setNodeToDeletedUrn]);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!loading && !searchQuery && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading, searchQuery]);

    const filteredChildren = !shouldDoAutoComplete
        ? data.filter((t) =>
              entityRegistry.getDisplayName(t.type, t).toLocaleLowerCase().includes(searchQuery.toLocaleLowerCase()),
          )
        : searchData;

    return {
        scrollRef,
        data: searchQuery ? filteredChildren : data,
        loading: loading || (shouldDoAutoComplete && autoCompleteLoading),
        searchQuery,
        setSearchQuery,
    };
}
