import i18next from 'i18next';

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
        get displayName() {
            return i18next.t('shared.query-builder:prop.type');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.typeDesc');
        },
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: VIEW_ENTITY_TYPES.map((e) => ({ id: e.id, displayName: e.displayName })),
        },
    },
    {
        id: 'typeNames',
        get displayName() {
            return i18next.t('entity.views:prop.subType');
        },
        get description() {
            return i18next.t('entity.views:prop.subTypeDesc');
        },
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            aggregationField: 'typeNames',
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'platform',
        get displayName() {
            return i18next.t('shared.query-builder:prop.platform');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.platformDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataPlatform],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'owners',
        get displayName() {
            return i18next.t('entity.views:prop.owner');
        },
        get description() {
            return i18next.t('entity.views:prop.ownerDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'domains',
        get displayName() {
            return i18next.t('shared.query-builder:prop.domain');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.domainDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Domain],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'dataProducts',
        get displayName() {
            return i18next.t('entity.views:prop.dataProduct');
        },
        get description() {
            return i18next.t('entity.views:prop.dataProductDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.DataProduct],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'tags',
        get displayName() {
            return i18next.t('shared.query-builder:prop.tags');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.tagsAppliedDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'glossaryTerms',
        get displayName() {
            return i18next.t('shared.query-builder:prop.glossaryTerms');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.glossaryTermsAppliedDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'container',
        get displayName() {
            return i18next.t('shared.query-builder:prop.container');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.containerDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Container],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'fieldPaths',
        get displayName() {
            return i18next.t('entity.views:prop.columnName');
        },
        get description() {
            return i18next.t('entity.views:prop.columnNameDesc');
        },
        valueType: ValueTypeId.STRING,
    },
    {
        id: 'fieldTags',
        get displayName() {
            return i18next.t('entity.views:prop.columnTag');
        },
        get description() {
            return i18next.t('entity.views:prop.columnTagDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.Tag],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'fieldGlossaryTerms',
        get displayName() {
            return i18next.t('entity.views:prop.columnGlossaryTerm');
        },
        get description() {
            return i18next.t('entity.views:prop.columnGlossaryTermDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.GlossaryTerm],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'hasDescription',
        get displayName() {
            return i18next.t('entity.views:prop.hasDescription');
        },
        get description() {
            return i18next.t('entity.views:prop.hasDescriptionDesc');
        },
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'removed',
        get displayName() {
            return i18next.t('entity.views:prop.softDeleted');
        },
        get description() {
            return i18next.t('entity.views:prop.softDeletedDesc');
        },
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'hasActiveIncidents',
        get displayName() {
            return i18next.t('entity.views:prop.hasActiveIncidents');
        },
        get description() {
            return i18next.t('entity.views:prop.hasActiveIncidentsDesc');
        },
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'hasFailingAssertions',
        get displayName() {
            return i18next.t('entity.views:prop.hasFailingAssertions');
        },
        get description() {
            return i18next.t('entity.views:prop.hasFailingAssertionsDesc');
        },
        valueType: ValueTypeId.BOOLEAN,
    },
    {
        id: 'origin',
        get displayName() {
            return i18next.t('entity.views:prop.environment');
        },
        get description() {
            return i18next.t('entity.views:prop.environmentDesc');
        },
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            mode: SelectInputMode.MULTIPLE,
            options: [
                {
                    id: 'PROD',
                    get displayName() {
                        return i18next.t('entity.views:prop.environmentProd');
                    },
                },
                {
                    id: 'DEV',
                    get displayName() {
                        return i18next.t('entity.views:prop.environmentDev');
                    },
                },
                {
                    id: 'STAGING',
                    get displayName() {
                        return i18next.t('entity.views:prop.environmentStaging');
                    },
                },
            ],
        },
    },
    {
        id: 'platformInstance',
        get displayName() {
            return i18next.t('entity.views:prop.platformInstance');
        },
        get description() {
            return i18next.t('entity.views:prop.platformInstanceDesc');
        },
        valueType: ValueTypeId.ENUM,
        valueOptions: {
            aggregationField: 'platformInstance',
            mode: SelectInputMode.MULTIPLE,
        },
    },
];
