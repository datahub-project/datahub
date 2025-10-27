import { useCallback, useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { Entity, EntityType, FilterOperator, SortOrder } from '@src/types.generated';

import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

export const DOMAIN_SELECTOR_COUNT = 25;

export function getDomainSelectorScrollInput(parentDomain: string | null, scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.Domain],
            orFilters: parentDomain
                ? [{ and: [{ field: 'parentDomain', values: [parentDomain] }] }]
                : [{ and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] }],
            count: DOMAIN_SELECTOR_COUNT,
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

interface Props {
    parentDomain?: string;
    skip?: boolean;
}

/**
 * Custom hook for infinite scroll domains specifically adapted for DomainSelector
 * Returns NestedSelectOption format domains and intersection observer ref
 */
export default function useInfiniteScrollDomains({ parentDomain, skip }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [hasInitialized, setHasInitialized] = useState(false);
    const [domains, setDomains] = useState<Entity[]>([]);
    const [domainsUrnsSet, setDomainsUrnsSet] = useState<Set<string>>(new Set());
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [hasMoreDomains, setHasMoreDomains] = useState(true);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: {
            ...getDomainSelectorScrollInput(parentDomain || null, scrollId),
        },
        skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'cache-and-network',
    });

    // Handle initial data and updates from scroll
    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const newResults = (scrollData.scrollAcrossEntities.searchResults
                .filter((r) => !domainsUrnsSet.has(r.entity.urn))
                .map((r) => r.entity)
                .filter((e) => e.type === EntityType.Domain) || []);

            if (newResults.length > 0) {
                setDomains((currDomains) => [...currDomains, ...newResults]);
                setDomainsUrnsSet((currSet) => {
                    const newSet = new Set(currSet);
                    newResults.forEach((r) => newSet.add(r.urn));
                    return newSet;
                });
            }

            // Check if we have more domains to load
            const nextScrollId = scrollData.scrollAcrossEntities?.nextScrollId;
            setHasMoreDomains(!!nextScrollId);
            setHasInitialized(true);
        }
    }, [scrollData, domainsUrnsSet]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    // Intersection observer for infinite scroll
    const [scrollRef, inView] = useInView({ triggerOnce: false, threshold: 0.1 });

    // Trigger loading more domains when scroll ref comes into view
    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView && hasMoreDomains) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading, hasMoreDomains]);

    // Convert domains to NestedSelectOption format
    const nestedSelectOptions = useMemo((): NestedSelectOption[] => {
        return domains.map((domain) => ({
            value: domain.urn,
            label: entityRegistry.getDisplayName(domain.type, domain),
            id: domain.urn,
            isParent: !!(domain as any)?.children?.total,
            parentValue: (domain as any)?.parentDomains?.domains?.[0]?.urn,
            entity: domain,
        }));
    }, [domains, entityRegistry]);

    // Function to manually load more domains (can be used for refresh)
    const loadMoreDomains = useCallback(() => {
        if (!loading && hasMoreDomains && nextScrollId) {
            setScrollId(nextScrollId);
        }
    }, [loading, hasMoreDomains, nextScrollId]);

    // Reset function to clear all data and start fresh
    const resetDomains = useCallback(() => {
        setDomains([]);
        setDomainsUrnsSet(new Set());
        setScrollId(null);
        setHasInitialized(false);
        setHasMoreDomains(true);
    }, []);

    return {
        domains: nestedSelectOptions,
        rawDomains: domains,
        hasInitialized,
        loading,
        error,
        hasMoreDomains,
        refetch,
        scrollRef,
        loadMoreDomains,
        resetDomains,
    };
}
