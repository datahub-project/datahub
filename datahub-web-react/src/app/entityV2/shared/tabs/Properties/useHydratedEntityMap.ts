import { useGetEntities } from '@src/app/sharedV2/useGetEntities';
import { Entity } from '@src/types.generated';
import { useMemo } from 'react';

export function useHydratedEntityMap(urns?: (string | undefined | null)[]) {
    // Get unique URNs
    const uniqueEntityUrns = useMemo(
        () => Array.from(new Set(urns?.filter((urn): urn is string => !!urn) || [])),
        [urns],
    );

    // Fetch entities
    const hydratedEntities = useGetEntities(uniqueEntityUrns);

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
