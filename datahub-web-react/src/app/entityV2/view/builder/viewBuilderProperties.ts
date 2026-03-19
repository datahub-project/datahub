import { VIEW_ENTITY_TYPES } from '@app/entityV2/view/builder/constants';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { EntityType } from '@types';

/**
 * View-specific properties for the Dynamic Filter tab in the View builder.
 * Matches the filter capabilities from VIEW_BUILDER_FIELDS while using
 * the query builder property format.
 */
export const viewBuilderProperties: Property[] = [
    {
        id: '_entityType',
        displayName: 'Type',
        description: 'The type of the asset.',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: VIEW_ENTITY_TYPES.map((e) => ({ id: e.id, displayName: e.displayName })),
        },
    },
    {
        id: 'typeNames',
        displayName: 'Sub Type',
        description: 'The sub type of the asset (e.g. Table, View, Topic).',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            aggregationField: 'typeNames',
            mode: SelectInputMode.MULTIPLE,
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
        id: 'owners',
        displayName: 'Owner',
        description: 'The owners of an asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
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
        id: 'dataProducts',
        displayName: 'Data Product',
        description: 'The data product the asset belongs to.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataProduct],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'tags',
        displayName: 'Tags',
        description: 'The tags applied to an asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms',
        displayName: 'Glossary Terms',
        description: 'The glossary terms applied to an asset.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
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
        id: 'fieldPaths',
        displayName: 'Column Name',
        description: 'The name of a schema field / column.',
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'fieldTags',
        displayName: 'Column Tag',
        description: 'Tags applied to a schema field / column.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'fieldGlossaryTerms',
        displayName: 'Column Glossary Term',
        description: 'Glossary terms applied to a schema field / column.',
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'hasDescription',
        displayName: 'Has Description',
        description: 'Whether the asset has a description.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'removed',
        displayName: 'Soft Deleted',
        description: 'Whether the asset has been soft deleted.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'hasActiveIncidents',
        displayName: 'Has Active Incidents',
        description: 'Whether the asset has active incidents.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'hasFailingAssertions',
        displayName: 'Has Failing Assertions',
        description: 'Whether the asset has failing data quality assertions.',
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'origin',
        displayName: 'Environment',
        description: 'The environment / origin of the asset (e.g. PROD, DEV).',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: [
                { id: 'PROD', displayName: 'Production' },
                { id: 'DEV', displayName: 'Development' },
                { id: 'STAGING', displayName: 'Staging' },
            ],
        },
    },
    {
        id: 'platformInstance',
        displayName: 'Platform Instance',
        description: 'The specific platform instance where the asset lives.',
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            aggregationField: 'platformInstance',
            mode: SelectInputMode.MULTIPLE,
        },
    },
];
