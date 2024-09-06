import { SelectInputMode, ValueTypeId } from '@src/app/tests/builder/steps/definition/builder/property/types/values';
import { EntityType } from '@src/types.generated';

export const properties = [
    {
        id: 'entityType',
        displayName: 'Type',
        description: 'The type of the asset.',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: [
                {
                    id: 'dataset',
                    displayName: 'Dataset',
                },
                {
                    id: 'dashboard',
                    displayName: 'Dashboard',
                },
                {
                    id: 'chart',
                    displayName: 'Chart',
                },
                {
                    id: 'dataJob',
                    displayName: 'Data Job (Task)',
                },
                {
                    id: 'dataFlow',
                    displayName: 'Data Flow (Pipeline)',
                },
                {
                    id: 'container',
                    displayName: 'Container',
                },
            ],
        },
    },
    {
        id: 'dataPlatformInstance.platform',
        displayName: 'Platform',
        description: 'The data platform where the asset lives.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'container.container',
        displayName: 'Container',
        description: 'The parent container of the asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.SINGLE,
        },
    },
    {
        id: 'domains.domains',
        displayName: 'Domain',
        description: 'The domain that the asset is a part of.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.MULTIPLE,
        },
    },
];
