/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
