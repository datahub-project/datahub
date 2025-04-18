import { Select } from 'antd';
import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../../useEntityRegistry';

const selectStyle = { width: 340 };

type Props = {
    entityTypes: EntityType[];
    selectedTypes: EntityType[];
    onChangeTypes: (newTypes: EntityType[]) => void;
};

export const EntityTypeSelect = ({ entityTypes, selectedTypes, onChangeTypes }: Props) => {
    const entityRegistry = useEntityRegistry();

    const onSelectEntityType = (value) => {
        const newEntities = [...selectedTypes, value];
        onChangeTypes(newEntities);
    };

    const onDeselectEntityType = (value) => {
        onChangeTypes(selectedTypes.filter((entityType) => entityType !== value));
    };

    return (
        <Select
            style={selectStyle}
            value={selectedTypes}
            mode="multiple"
            placeholder="Datasets, Dashboards, Charts..."
            onSelect={onSelectEntityType}
            onDeselect={onDeselectEntityType}
            data-testid="entity-type-select"
        >
            {Array.from(entityTypes).map((entityType) => (
                <Select.Option value={entityType} key={entityType}>
                    {entityRegistry.getCollectionName(entityType)}
                </Select.Option>
            ))}
        </Select>
    );
};
