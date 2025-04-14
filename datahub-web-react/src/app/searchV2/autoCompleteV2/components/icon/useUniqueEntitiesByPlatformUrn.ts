import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';
import { useMemo } from 'react';

export default function useUniqueEntitiesByPlatformUrn(entities: Entity[] | undefined): Entity[] {
    const entityRegistry = useEntityRegistryV2();

    return useMemo(() => {
        const platformUrnsOfEntities = (entities || [])
            .map((entity) => entityRegistry.getGenericEntityProperties(entity.type, entity)?.platform?.urn)
            .filter((platformUrn) => !!platformUrn);

        return (entities || []).filter(
            (_, index) =>
                index ===
                platformUrnsOfEntities.findIndex((platformUrn) => platformUrn === platformUrnsOfEntities[index]),
        );
    }, [entities, entityRegistry]);
}
