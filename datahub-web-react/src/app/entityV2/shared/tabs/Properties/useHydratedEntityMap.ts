import { useMemo } from 'react';

import { useGetEntities } from '@src/app/sharedV2/useGetEntities';
import { Entity } from '@src/types.generated';

/**
 * Hydrate the entities behind URN-valued structured property values so they render with a name and
 * icon. Only pass the URNs that are actually on screen: a single property can hold thousands of
 * values, and each one here becomes a full entity in one `getEntities` round trip. Lineage counts
 * and siblings are never needed for a name chip, so they are skipped.
 */
export function useHydratedEntityMap(urns?: (string | undefined | null)[]) {
    // Get unique URNs
    const uniqueEntityUrns = useMemo(
        () => Array.from(new Set(urns?.filter((urn): urn is string => !!urn) || [])),
        [urns],
    );

    // Fetch entities
    const { entities: hydratedEntities } = useGetEntities(uniqueEntityUrns, undefined, {
        skipLineage: true,
        skipSiblingsSearch: true,
    });

    // Create entity map
    const hydratedEntityMap = useMemo(
        () =>
            hydratedEntities.reduce<Record<string, Entity>>((acc, entity) => {
                acc[entity.urn] = entity;
                return acc;
            }, {}),
        [hydratedEntities],
    );

    return hydratedEntityMap;
}
