import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { EntityType } from '@src/types.generated';

export const properties = [
    {
        id: '_entityType',
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
                {
                    id: 'domain',
                    displayName: 'Domain',
                },
                {
                    id: 'dataProduct',
                    displayName: 'Data Product',
                },
                {
                    id: 'glossaryTerm',
                    displayName: 'Glossary Term',
                },
                {
                    id: 'glossaryNode',
                    displayName: 'Term Group',
                },
                {
                    id: 'mlModel',
                    displayName: 'ML Model',
                },
                {
                    id: 'mlModelGroup',
                    displayName: 'ML Model Group',
                },
                {
                    id: 'mlFeature',
                    displayName: 'ML Feature',
                },
                {
                    id: 'mlFeatureTable',
                    displayName: 'ML Feature Table',
                },
                {
                    id: 'mlPrimaryKey',
                    displayName: 'ML Primary Key',
                },
            ],
        },
    },
    {
        id: 'platform',
        displayName: 'Platform',
        description: 'The data platform where the asset lives.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'container',
        displayName: 'Container',
        description: 'The parent container of the asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'domains',
        displayName: 'Domain',
        description: 'The domain that the asset is a part of.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms',
        displayName: 'Glossary Terms',
        description: 'The glossary terms applied to an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'tags',
        displayName: 'Tags',
        description: 'The tags applied to an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'owners',
        displayName: 'Owned By',
        description: 'The owners of an asset',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'urn',
        displayName: 'Asset',
        description: 'The specific asset itself',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [
                EntityType.Dataset,
                EntityType.Dashboard,
                EntityType.Chart,
                EntityType.Mlmodel,
                EntityType.MlmodelGroup,
                EntityType.MlfeatureTable,
                EntityType.Mlfeature,
                EntityType.MlprimaryKey,
                EntityType.DataFlow,
                EntityType.DataJob,
                EntityType.GlossaryTerm,
                EntityType.GlossaryNode,
                EntityType.Container,
                EntityType.Domain,
                EntityType.DataProduct,
            ],
            mode: SelectInputMode.MULTIPLE,
        },
    },
];
