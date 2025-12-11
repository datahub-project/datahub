/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { useGetEntities } from '@src/app/sharedV2/useGetEntities';
import { Entity } from '@src/types.generated';

export function useHydratedEntityMap(urns?: (string | undefined | null)[]) {
    // Get unique URNs
    const uniqueEntityUrns = useMemo(
        () => Array.from(new Set(urns?.filter((urn): urn is string => !!urn) || [])),
        [urns],
    );

    // Fetch entities
    const { entities: hydratedEntities } = useGetEntities(uniqueEntityUrns);

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
