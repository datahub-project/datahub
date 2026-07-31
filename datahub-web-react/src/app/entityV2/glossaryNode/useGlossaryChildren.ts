import { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';
import { useDebounce } from 'react-use';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { DEFAULT_GLOSSARY_CHILDREN_COUNT, getGlossaryChildrenScrollInput } from '@app/glossaryV2/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { useGetAutoCompleteMultipleResultsQuery, useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

const GLOSSARY_CHILDREN_COUNT = DEFAULT_GLOSSARY_CHILDREN_COUNT;

interface Props {
    entityUrn?: string;
    skip?: boolean;
}

export default function useGlossaryChildren({ entityUrn, skip }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const {
        nodeToNewEntity,
        setNodeToNewEntity,
        setNodeToDeletedUrn,
        nodeToDeletedUrn,
        urnsToUpdate,
        setUrnsToUpdate,
    } = useGlossaryEntityData();
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [query, setQuery] = useState<string>(''); // query to use in auto-complete. gets debounced
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [searchData, setSearchData] = useState<Entity[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [data, setData] = useState<Entity[]>([]);
    const {
        data: scrollData,
        loading,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: getGlossaryChildrenScrollInput(entityUrn || '', scrollId),
        skip: !entityUrn || skip,
        notifyOnNetworkStatusChange: true,
    });
    const shouldDoAutoComplete = data.length >= GLOSSARY_CHILDREN_COUNT;

    // Handle initial data and updates from scroll.
    //
    // We MERGE rather than just append: any URN that comes back from the scroll query and is
    // already in `data` gets REPLACED with the fresh entity reference, while previously-seen
    // URNs that aren't in the fresh results (e.g. brand-new optimistic entries that the search
    // index hasn't picked up yet) are preserved. This is what makes both flows work without
    // stomping on each other:
    //   - Edits: when `urnsToUpdate` triggers a `refetch()`, the fresh response carries updated
    //     name/color/displayProperties, and the merge swaps the stale snapshot for the new one.
    //   - Creates: optimistic entries pushed via `nodeToNewEntity` survive subsequent refetches
    //     until the search index catches up and starts returning them — at which point the
    //     merge swaps the optimistic copy for the canonical server-side fields.
    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults.map((r) => r.entity);
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setData((currData) => {
                const updated = currData.map((e) => freshByUrn.get(e.urn) || e);
                const seenUrns = new Set(updated.map((e) => e.urn));
                const additions = fresh.filter((e) => !seenUrns.has(e.urn));
                if (additions.length === 0 && updated.every((e, i) => e === currData[i])) {
                    // No change — preserve referential equality so consumers don't re-render.
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

    // Refresh existing children when the parent is signaled via `urnsToUpdate` — e.g. a child
    // had its name, color, or other displayProperties edited. We trigger a `refetch()` and let
    // the scroll-results effect above MERGE the fresh entities into local `data` (replacing
    // stale snapshots by URN). This explicitly does NOT clear `data` / `dataUrnsSet` — doing so
    // would wipe any previously-added optimistic entries from `nodeToNewEntity` that the
    // search index hasn't caught up to yet (e.g. siblings created seconds earlier).
    useEffect(() => {
        if (entityUrn && urnsToUpdate.includes(entityUrn)) {
            refetch(getGlossaryChildrenScrollInput(entityUrn, scrollId));
            // Functional setter so we don't strip the wrong subset when multiple parents are
            // signaled in `urnsToUpdate` for the same render — `urnsToUpdate` from the closure
            // may not include them all by the time React commits this update.
            setUrnsToUpdate((prev) => prev.filter((urn) => urn !== entityUrn));
        }
    }, [entityUrn, urnsToUpdate, setUrnsToUpdate, refetch, scrollId]);

    // update when new entity is added.
    // Intentionally excludes `data` from the deps — it's not read in the body and including it
    // would re-run on every scroll-merge tick, racing with the optimistic insertion below.
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
        refetch,
    };
}
