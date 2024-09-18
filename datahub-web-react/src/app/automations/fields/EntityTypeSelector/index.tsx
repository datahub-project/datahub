import React from 'react';
import { Select } from 'antd';

import type { EntityType } from '@src/types.generated';
import type { ComponentBaseProps } from '@app/automations/types';

import { useEntityRegistry } from '@app/useEntityRegistry';

// State Type (ensures the state is correctly applied across templates)
export type EntityTypeSelectorStateType = {
    entities: EntityType[];
};

// Component
export const EntityTypeSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { entities } = state as EntityTypeSelectorStateType;

    // Defined in @app/automations/fields/index
    const { entityTypes } = props;

    // Get selectables entities
    const entityRegistry = useEntityRegistry();
    let selectableEntities = entityRegistry.getEntities();

    // Filter out custom entity types
    if (entityTypes.length > 0) {
        selectableEntities = selectableEntities.filter((entity) => entityTypes.includes(entity.type as any));
    }

    // Placeholder generated from the selectable entities
    // Truncates after the first 3 entities
    const placeholder = selectableEntities
        .slice(0, 3)
        .map((entity) => entityRegistry.getCollectionName(entity.type))
        .join(', ');

    const orBoth = selectableEntities.length === 2 ? ' or both' : '';

    return (
        <Select
            value={entities || []}
            mode="multiple"
            placeholder={`Select ${placeholder}${orBoth}…`}
            onSelect={(entityType: EntityType) => {
                passStateToParent({ entities: [...entities, entityType] });
            }}
            onDeselect={(entityType: EntityType) => {
                passStateToParent({ entities: entities.filter((type) => type !== entityType) });
            }}
        >
            {Array.from(selectableEntities).map((entity) => (
                <Select.Option value={entity.type} key={entity.type}>
                    {entityRegistry.getCollectionName(entity.type)}
                </Select.Option>
            ))}
        </Select>
    );
};
