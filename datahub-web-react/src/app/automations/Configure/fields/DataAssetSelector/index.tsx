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

import { InputWithButton } from '../../components';
import { SecondaryButton } from '../../../sharedComponents';

interface Props {
	dataAssetSelected: any;
	setDataAssetSelected: (dataAssetSelected: any) => void;
}

export const DataAssetSelector = ({ dataAssetSelected, setDataAssetSelected }: Props) => {
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

	const hasAssetsSelected = dataAssetSelected && dataAssetSelected.length > 0;

	return (
		<InputWithButton>
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
			{hasAssetsSelected && (
				<SecondaryButton onClick={() => { }} disabled>
					Preview Source Set
				</SecondaryButton>
			)}
		</InputWithButton>
	);
}