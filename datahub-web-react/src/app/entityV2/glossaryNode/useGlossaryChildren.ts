import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';
import { useDebounce } from 'react-use';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { useGlossarySidebarFilters } from '@app/glossaryV2/glossarySidebarFilters/GlossarySidebarFiltersContext';
import { getGlossaryScrollInput } from '@app/glossaryV2/glossarySidebarFilters/getGlossaryScrollInput';
import {
    DEFAULT_GLOSSARY_SIDEBAR_SORT,
    GlossarySidebarSortValue,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';
import { DEFAULT_GLOSSARY_CHILDREN_COUNT } from '@app/glossaryV2/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { useGetAutoCompleteMultipleResultsQuery, useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

const GLOSSARY_CHILDREN_COUNT = DEFAULT_GLOSSARY_CHILDREN_COUNT;

interface Props {
    entityUrn?: string;
    skip?: boolean;
    /** Override sort when not using sidebar context (tests / pickers). */
    sort?: GlossarySidebarSortValue;
}

export default function useGlossaryChildren({ entityUrn, skip, sort: sortOverride }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const { sortSelection } = useGlossarySidebarFilters();
    const sort = sortOverride ?? sortSelection ?? DEFAULT_GLOSSARY_SIDEBAR_SORT;
    const {
        nodeToNewEntity,
        setNodeToNewEntity,
        setNodeToDeletedUrn,
        nodeToDeletedUrn,
        urnsToUpdate,
        setUrnsToUpdate,
    } = useGlossaryEntityData();
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [query, setQuery] = useState<string>('');
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [searchData, setSearchData] = useState<Entity[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [data, setData] = useState<Entity[]>([]);

    const scrollVariables = useMemo(
        () =>
            getGlossaryScrollInput({
                parentNode: entityUrn || null,
                scrollId,
                sort,
                sortTypeBeforeName: true,
            }),
        [entityUrn, scrollId, sort],
    );

    useEffect(() => {
        setData([]);
        setDataUrnsSet(new Set());
        setScrollId(null);
    }, [entityUrn, sort]);

    const {
        data: scrollData,
        loading,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: scrollVariables,
        skip: !entityUrn || skip,
        notifyOnNetworkStatusChange: true,
    });
    const shouldDoAutoComplete = data.length >= GLOSSARY_CHILDREN_COUNT;

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults.map((r) => r.entity);
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setData((currData) => {
                const updated = currData.map((e) => freshByUrn.get(e.urn) || e);
                const seenUrns = new Set(updated.map((e) => e.urn));
                const additions = fresh.filter((e) => !seenUrns.has(e.urn));
                if (additions.length === 0 && updated.every((e, i) => e === currData[i])) {
                    return currData;
                }
                return [...updated, ...additions];
            });
            setDataUrnsSet((currSet) => {
                if (fresh.every((e) => currSet.has(e.urn))) return currSet;
                const next = new Set(currSet);
                fresh.forEach((e) => next.add(e.urn));
                return next;
            });
        }
    }, [scrollData, entityUrn]);

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
            const results = d.autoCompleteForMultiple?.suggestions?.flatMap((s) => s.entities);
            if (results) {
                setSearchData(results);
            }
        },
    });

    useEffect(() => {
        if (entityUrn && urnsToUpdate.includes(entityUrn)) {
            refetch(scrollVariables);
            setUrnsToUpdate((prev) => prev.filter((urn) => urn !== entityUrn));
        }
    }, [entityUrn, urnsToUpdate, setUrnsToUpdate, refetch, scrollVariables]);

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
        refetch: () => refetch(scrollVariables),
    };
}
