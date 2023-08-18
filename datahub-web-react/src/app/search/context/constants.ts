import { EntityType, MatchedField, SortOrder } from '../../../types.generated';

export const RELEVANCE = 'relevance';
export const NAME_FIELD = 'name';
export const LAST_OPERATION_TIME_FIELD = 'lastOperationTime';

export const DEFAULT_SORT_OPTION = RELEVANCE;

export const SORT_OPTIONS = {
    [RELEVANCE]: { label: 'Relevance', field: RELEVANCE, sortOrder: SortOrder.Descending },
    [`${NAME_FIELD}_${SortOrder.Ascending}`]: { label: 'A to Z', field: NAME_FIELD, sortOrder: SortOrder.Ascending },
    [`${NAME_FIELD}_${SortOrder.Descending}`]: { label: 'Z to A', field: NAME_FIELD, sortOrder: SortOrder.Descending },
    [`${LAST_OPERATION_TIME_FIELD}_${SortOrder.Descending}`]: {
        label: 'Last Modified in Platform',
        field: LAST_OPERATION_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    },
};

export type MatchedFieldName =
    | 'urn'
    | 'name'
    | 'qualifiedName'
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
    | 'editedFieldGlossaryTerms'
    | 'fieldLabels'
    | 'fieldPaths';

export type MatchedFieldConfig = {
    name: MatchedFieldName;
    normalizedName?: MatchedFieldName;
    label: string;
    showInMatchedFieldList?: boolean;
};

const DEFAULT_MATCHED_FIELD_CONFIG: Record<MatchedFieldName, MatchedFieldConfig> = {
    urn: {
        name: 'urn',
        label: 'urn',
    },
    name: {
        name: 'name',
        normalizedName: 'name',
        label: 'name',
    },
    qualifiedName: {
        name: 'qualifiedName',
        normalizedName: 'name',
        label: 'qualified name',
    },
    displayName: {
        name: 'displayName',
        normalizedName: 'name',
        label: 'display name',
    },
    title: {
        name: 'title',
        label: 'title',
    },
    description: {
        name: 'description',
        normalizedName: 'description',
        label: 'description',
    },
    editedDescription: {
        name: 'editedDescription',
        normalizedName: 'description',
        label: 'edited description',
    },
    editedFieldDescriptions: {
        name: 'editedFieldDescriptions',
        label: 'column description',
        showInMatchedFieldList: true,
    },
    fieldDescriptions: {
        name: 'fieldDescriptions',
        label: 'column descriptions',
        showInMatchedFieldList: true,
    },
    tags: {
        name: 'tags',
        label: 'tags',
    },
    fieldTags: {
        name: 'fieldTags',
        label: 'column tag',
        showInMatchedFieldList: true,
    },
    editedFieldTags: {
        name: 'editedFieldTags',
        label: 'column tag',
        showInMatchedFieldList: true,
    },
    glossaryTerms: {
        name: 'glossaryTerms',
        label: 'term',
    },
    editedFieldGlossaryTerms: {
        name: 'editedFieldGlossaryTerms',
        label: 'column term',
        showInMatchedFieldList: true,
    },
    fieldLabels: {
        name: 'fieldLabels',
        label: 'label',
        showInMatchedFieldList: true,
    },
    fieldPaths: {
        name: 'fieldPaths',
        label: 'column',
        showInMatchedFieldList: true,
    },
} as const;

export const CHART_DASHBOARD_FIELD_CONFIG: Record<MatchedFieldName, MatchedFieldConfig> = {
    ...DEFAULT_MATCHED_FIELD_CONFIG,
    title: {
        ...DEFAULT_MATCHED_FIELD_CONFIG.title,
        normalizedName: 'name',
    },
} as const;

export const MATCHED_FIELD_CONFIG = {
    [EntityType.Chart]: CHART_DASHBOARD_FIELD_CONFIG,
    [EntityType.Dashboard]: CHART_DASHBOARD_FIELD_CONFIG,
    DEFAULT: DEFAULT_MATCHED_FIELD_CONFIG,
} as const;

export type MatchesGroupedByFieldName = {
    fieldName: string;
    matchedFields: Array<MatchedField>;
};
