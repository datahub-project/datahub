import { useMemo } from 'react';

import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';

export default function useUniqueEntitiesByPlatformUrn(entities: Entity[] | undefined): Entity[] {
    const entityRegistry = useEntityRegistryV2();

    return useMemo(() => {
        const seenPlatformUrns = new Set<string>();
        const result: Entity[] = [];

        (entities || [])
            .map((entity) => ({
                entity,
                platformUrn: entityRegistry.getGenericEntityProperties(entity.type, entity)?.platform?.urn,
            }))
            .forEach((value) => {
                if (value.platformUrn !== undefined && !seenPlatformUrns.has(value.platformUrn)) {
                    seenPlatformUrns.add(value.platformUrn);
                    result.push(value.entity);
                }
            });

        return result;
    }, [entities, entityRegistry]);
}
