import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { getGlossaryScrollInput } from '@app/glossaryV2/glossarySidebarFilters/getGlossaryScrollInput';
import {
    DEFAULT_GLOSSARY_SIDEBAR_SORT,
    GlossarySidebarSortValue,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';

import { useScrollAcrossEntitiesQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

interface Props {
    parentNode?: string | null;
    skip?: boolean;
    sort?: GlossarySidebarSortValue;
    selectedOwnerUrns?: ReadonlyArray<string>;
    selectedTagUrns?: ReadonlyArray<string>;
    selectedDomainUrns?: ReadonlyArray<string>;
    ignoreParentScope?: boolean;
    /** Flat filter mode: name-only sort across nodes + terms. */
    sortTypeBeforeName?: boolean;
}

/**
 * Scroll loader for glossary sidebar tree roots / flat filtered results.
 * Children under an expanded node still use `useGlossaryChildren`.
 */
export default function useScrollGlossaryEntities({
    parentNode = null,
    skip,
    sort = DEFAULT_GLOSSARY_SIDEBAR_SORT,
    selectedOwnerUrns,
    selectedTagUrns,
    selectedDomainUrns,
    ignoreParentScope,
    sortTypeBeforeName = true,
}: Props) {
    const [hasInitialized, setHasInitialized] = useState(false);
    const [data, setData] = useState<Entity[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [scrollId, setScrollId] = useState<string | null>(null);

    const ownerKey = useMemo(
        () => (selectedOwnerUrns ? [...selectedOwnerUrns].sort().join(',') : ''),
        [selectedOwnerUrns],
    );
    const tagKey = useMemo(() => (selectedTagUrns ? [...selectedTagUrns].sort().join(',') : ''), [selectedTagUrns]);
    const domainKey = useMemo(
        () => (selectedDomainUrns ? [...selectedDomainUrns].sort().join(',') : ''),
        [selectedDomainUrns],
    );

    useEffect(() => {
        setData([]);
        setDataUrnsSet(new Set());
        setScrollId(null);
        setHasInitialized(false);
    }, [parentNode, ownerKey, tagKey, domainKey, ignoreParentScope, sort, sortTypeBeforeName]);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: getGlossaryScrollInput({
            parentNode,
            scrollId,
            sort,
            selectedOwnerUrns,
            selectedTagUrns,
            selectedDomainUrns,
            ignoreParentScope,
            sortTypeBeforeName,
        }),
        skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'cache-and-network',
    });

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const newResults = scrollData.scrollAcrossEntities.searchResults
                .filter((r) => !dataUrnsSet.has(r.entity.urn))
                .map((r) => r.entity)
                .filter((e) => e.type === EntityType.GlossaryNode || e.type === EntityType.GlossaryTerm);

            if (newResults.length > 0) {
                setData((currData) => [...currData, ...newResults]);
                setDataUrnsSet((currSet) => {
                    const newSet = new Set(currSet);
                    newResults.forEach((r) => newSet.add(r.urn));
                    return newSet;
                });
            }
            setHasInitialized(true);
        }
    }, [scrollData, dataUrnsSet]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;
    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    return {
        entities: data,
        hasInitialized,
        loading,
        error,
        refetch,
        scrollRef,
    };
}
