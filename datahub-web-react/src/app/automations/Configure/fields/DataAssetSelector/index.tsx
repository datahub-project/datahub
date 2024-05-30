/*
* Data Asset Selector 
* User 1st selects the type of entities(s) they want to test 
* Then they can optionally filter the entities by a set of conditions
*/

import React from 'react';

import { Select } from 'antd';

import { useEntityRegistry } from '../../../../useEntityRegistry';
import { EntityType } from '../../../../../types.generated';
import { EntityCapabilityType } from '../../../../entityV2/Entity';

export const DataAssetSelector = ({ dataAssetSelected, setDataAssetSelected }: any) => {
	// Get selectables entities
	const entityRegistry = useEntityRegistry();
	const selectableEntities: EntityType[] = Array.from(
		entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.TEST as any),
	);

	const onSelectEntityType = (entityType: EntityType) => {
		setDataAssetSelected([...dataAssetSelected, entityType]);
	}

	const onDeselectEntityType = (entityType: EntityType) => {
		setDataAssetSelected(dataAssetSelected.filter((type) => type !== entityType));
	}

	return (
		<Select
			value={dataAssetSelected || []}
			mode="multiple"
			placeholder="Datasets, Dashboards, Charts..."
			onSelect={onSelectEntityType}
			onDeselect={onDeselectEntityType}
		>
			{Array.from(selectableEntities).map((entityType) => (
				<Select.Option value={entityType} key={entityType}>
					{entityRegistry.getCollectionName(entityType)}
				</Select.Option>
			))}
		</Select>
	);
}