import i18next from 'i18next';

import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { EntityType } from '@src/types.generated';

export const properties = [
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
            options: [
                { id: 'dataset', displayName: 'Dataset' },
                { id: 'dashboard', displayName: 'Dashboard' },
                { id: 'chart', displayName: 'Chart' },
                { id: 'dataJob', displayName: 'Data Job (Task)' },
                { id: 'dataFlow', displayName: 'Data Flow (Pipeline)' },
                { id: 'container', displayName: 'Container' },
                { id: 'domain', displayName: 'Domain' },
                { id: 'dataProduct', displayName: 'Data Product' },
                { id: 'glossaryTerm', displayName: 'Glossary Term' },
                { id: 'glossaryNode', displayName: 'Term Group' },
                { id: 'mlModel', displayName: 'ML Model' },
                { id: 'mlModelGroup', displayName: 'ML Model Group' },
                { id: 'mlFeature', displayName: 'ML Feature' },
                { id: 'mlFeatureTable', displayName: 'ML Feature Table' },
                { id: 'mlPrimaryKey', displayName: 'ML Primary Key' },
            ],
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
        id: 'owners',
        get displayName() {
            return i18next.t('shared.query-builder:prop.ownedBy');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.ownedByDesc');
        },
        valueType: ValueTypeId.URN,
        valueOptions: {
            entityTypes: [EntityType.CorpUser, EntityType.CorpGroup],
            mode: SelectInputMode.MULTIPLE,
        },
    },
    {
        id: 'urn',
        get displayName() {
            return i18next.t('shared.query-builder:prop.asset');
        },
        get description() {
            return i18next.t('shared.query-builder:prop.assetDesc');
        },
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
