import { useCallback, useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

interface StructuredPropertyResult {
    value: string;
    label: string;
}

const PROPERTIES_BATCH_SIZE = 100;

/**
 * Hook for infinite scroll pagination of structured properties
 * Uses cursor-based pagination via scrollAcrossEntities query
 */
export function useInfiniteScrollStructuredProperties(searchQuery: string) {
    const [properties, setProperties] = useState<StructuredPropertyResult[]>([]);
    const [propertyUrnsSet, setPropertyUrnsSet] = useState<Set<string>>(new Set());
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [hasMoreProperties, setHasMoreProperties] = useState(true);
    const [hasInitialized, setHasInitialized] = useState(false);
    const [lastQueryProcessed, setLastQueryProcessed] = useState<string | null>(searchQuery || '*');

    const {
        data: scrollData,
        loading,
        error,
    } = useScrollAcrossEntitiesQuery({
        variables: {
            input: {
                scrollId,
                query: searchQuery || '*',
                types: [EntityType.StructuredProperty],
                count: PROPERTIES_BATCH_SIZE,
            },
        },
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'cache-and-network',
    });

    // Handle initial data and updates from scroll
    useEffect(() => {
        const currentQuery = searchQuery || '*';
        // Only merge results if they belong to the current query and lastQueryProcessed is set
        if (
            scrollData?.scrollAcrossEntities?.searchResults &&
            lastQueryProcessed !== null &&
            lastQueryProcessed === currentQuery
        ) {
            const newResults = scrollData.scrollAcrossEntities.searchResults
                .filter((r) => !propertyUrnsSet.has(r.entity.urn))
                .map((r) => ({
                    value: r.entity.urn,
                    label: (r.entity as any).definition?.displayName || r.entity.urn,
                }));

            if (newResults.length > 0) {
                setProperties((currProps) => [...currProps, ...newResults]);
                setPropertyUrnsSet((currSet) => {
                    const newSet = new Set(currSet);
                    newResults.forEach((p) => newSet.add(p.value));
                    return newSet;
                });
            }

            const nextScrollId = scrollData.scrollAcrossEntities?.nextScrollId;
            setHasMoreProperties(!!nextScrollId);
            setHasInitialized(true);
        }
    }, [scrollData, propertyUrnsSet, lastQueryProcessed, searchQuery]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    // Intersection observer for infinite scroll
    const [scrollRef, inView] = useInView({ triggerOnce: false, threshold: 0.1 });

    // Trigger loading more when scroll ref comes into view
    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView && hasMoreProperties) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading, hasMoreProperties]);

    // Reset when search query changes
    useEffect(() => {
        setProperties([]);
        setPropertyUrnsSet(new Set());
        setScrollId(null);
        setHasMoreProperties(true);
        setHasInitialized(false);
        setLastQueryProcessed(searchQuery || '*');
    }, [searchQuery]);

    const reset = useCallback(() => {
        setProperties([]);
        setPropertyUrnsSet(new Set());
        setScrollId(null);
        setHasInitialized(false);
        setHasMoreProperties(true);
        setLastQueryProcessed(null);
    }, []);

    return {
        properties,
        scrollRef,
        loading,
        error,
        hasMore: hasMoreProperties,
        hasInitialized,
        reset,
    };
}
