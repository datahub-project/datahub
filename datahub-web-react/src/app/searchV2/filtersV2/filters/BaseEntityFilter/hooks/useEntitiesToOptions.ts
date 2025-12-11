/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';

import { BaseEntitySelectOption } from '@app/searchV2/filtersV2/filters/BaseEntityFilter/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';

export default function useConvertEntitiesToOptions() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback(
        (entities: Entity[]): BaseEntitySelectOption[] => {
            return entities.map((entity) => {
                const displayName = entityRegistry.getDisplayName(entity.type, entity);

                return {
                    value: entity.urn,
                    label: displayName,
                    displayName,
                    entity,
                };
            });
        },
        [entityRegistry],
    );
}
