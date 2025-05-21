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
