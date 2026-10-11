import i18next from 'i18next';

import { EntityType, MatchedField } from '@types';

export type MatchedFieldName =
    | 'urn'
    | 'name'
    | 'displayName'
    | 'title'
    | 'description'
    | 'editedDescription'
    | 'editedFieldDescriptions'
    | 'fieldDescriptions'
    | 'tags'
    | 'fieldTags'
    | 'editedFieldTags'
    | 'glossaryTerms'
    | 'fieldGlossaryTerms'
    | 'editedFieldGlossaryTerms'
    | 'fieldLabels'
    | 'fieldPaths';

export type MatchedFieldConfig = {
    name: MatchedFieldName;
    groupInto?: MatchedFieldName;
    label: string;
    showInMatchedFieldList?: boolean;
};

function getDefaultMatchedFieldConfig(): Array<MatchedFieldConfig> {
    return [
        {
            name: 'urn',
            label: 'urn' /* untranslated-text -- technical identifier, not UI prose */,
        },
        {
            name: 'title',
            label: 'title' /* untranslated-text -- technical field name, not UI prose */,
        },
        {
            name: 'displayName',
            groupInto: 'name',
            label: i18next.t('search:matches.field.displayName'),
        },
        {
            name: 'name',
            groupInto: 'name',
            label: i18next.t('search:matches.field.name'),
        },
        {
            name: 'editedDescription',
            groupInto: 'description',
            label: i18next.t('search:matches.field.description'),
        },
        {
            name: 'description',
            groupInto: 'description',
            label: i18next.t('search:matches.field.description'),
        },
        {
            name: 'editedFieldDescriptions',
            groupInto: 'fieldDescriptions',
            label: i18next.t('search:matches.field.columnDescription'),
            showInMatchedFieldList: true,
        },
        {
            name: 'fieldDescriptions',
            groupInto: 'fieldDescriptions',
            label: i18next.t('search:matches.field.columnDescription'),
            showInMatchedFieldList: true,
        },
        {
            name: 'tags',
            label: i18next.t('search:matches.field.tag'),
        },
        {
            name: 'editedFieldTags',
            groupInto: 'fieldTags',
            label: i18next.t('search:matches.field.columnTag'),
            showInMatchedFieldList: true,
        },
        {
            name: 'fieldTags',
            groupInto: 'fieldTags',
            label: i18next.t('search:matches.field.columnTag'),
            showInMatchedFieldList: true,
        },
        {
            name: 'glossaryTerms',
            label: i18next.t('search:matches.field.term'),
        },
        {
            name: 'editedFieldGlossaryTerms',
            groupInto: 'fieldGlossaryTerms',
            label: i18next.t('search:matches.field.columnTerm'),
            showInMatchedFieldList: true,
        },
        {
            name: 'fieldGlossaryTerms',
            groupInto: 'fieldGlossaryTerms',
            label: i18next.t('search:matches.field.columnTerm'),
            showInMatchedFieldList: true,
        },
        {
            name: 'fieldLabels',
            label: i18next.t('search:matches.field.label'),
            showInMatchedFieldList: true,
        },
        {
            name: 'fieldPaths',
            label: i18next.t('search:matches.field.column'),
            showInMatchedFieldList: true,
        },
    ];
}

function getChartDashboardFieldConfig(): Array<MatchedFieldConfig> {
    return getDefaultMatchedFieldConfig().map((config) =>
        config.name === 'title' ? { ...config, groupInto: 'name' } : config,
    );
}

export function getMatchedFieldConfig(entityType: EntityType | undefined): Array<MatchedFieldConfig> {
    if (entityType === EntityType.Chart || entityType === EntityType.Dashboard) {
        return getChartDashboardFieldConfig();
    }
    return getDefaultMatchedFieldConfig();
}

export type MatchesGroupedByFieldName = {
    fieldName: string;
    matchedFields: Array<MatchedField>;
};
