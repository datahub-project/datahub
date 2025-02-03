import { EntityType, MatchedField } from '../../../types.generated';

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

const DEFAULT_MATCHED_FIELD_CONFIG: Array<MatchedFieldConfig> = [
    {
        name: 'urn',
        label: 'urn',
    },
    {
        name: 'title',
        label: 'title',
    },
    {
        name: 'displayName',
        groupInto: 'name',
        label: 'display name',
    },
    {
        name: 'name',
        groupInto: 'name',
        label: 'name',
    },
    {
        name: 'editedDescription',
        groupInto: 'description',
        label: 'description',
    },
    {
        name: 'description',
        groupInto: 'description',
        label: 'description',
    },
    {
        name: 'editedFieldDescriptions',
        groupInto: 'fieldDescriptions',
        label: 'column description',
        showInMatchedFieldList: true,
    },
    {
        name: 'fieldDescriptions',
        groupInto: 'fieldDescriptions',
        label: 'column description',
        showInMatchedFieldList: true,
    },
    {
        name: 'tags',
        label: 'tag',
    },
    {
        name: 'editedFieldTags',
        groupInto: 'fieldTags',
        label: 'column tag',
        showInMatchedFieldList: true,
    },
    {
        name: 'fieldTags',
        groupInto: 'fieldTags',
        label: 'column tag',
        showInMatchedFieldList: true,
    },
    {
        name: 'glossaryTerms',
        label: 'term',
    },
    {
        name: 'editedFieldGlossaryTerms',
        groupInto: 'fieldGlossaryTerms',
        label: 'column term',
        showInMatchedFieldList: true,
    },
    {
        name: 'fieldGlossaryTerms',
        groupInto: 'fieldGlossaryTerms',
        label: 'column term',
        showInMatchedFieldList: true,
    },
    {
        name: 'fieldLabels',
        label: 'label',
        showInMatchedFieldList: true,
    },
    {
        name: 'fieldPaths',
        label: 'column',
        showInMatchedFieldList: true,
    },
];

export const CHART_DASHBOARD_FIELD_CONFIG: Array<MatchedFieldConfig> = DEFAULT_MATCHED_FIELD_CONFIG.map((config) => {
    if (config.name === 'title') return { ...config, groupInto: 'name' };
    return config;
});

export const MATCHED_FIELD_CONFIG = {
    [EntityType.Chart]: CHART_DASHBOARD_FIELD_CONFIG,
    [EntityType.Dashboard]: CHART_DASHBOARD_FIELD_CONFIG,
    DEFAULT: DEFAULT_MATCHED_FIELD_CONFIG,
} as const;

export type MatchesGroupedByFieldName = {
    fieldName: string;
    matchedFields: Array<MatchedField>;
};

export const HIGHLIGHTABLE_ENTITY_TYPES = [EntityType.Tag, EntityType.GlossaryTerm];
