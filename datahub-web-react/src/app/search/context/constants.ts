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

export type MatchFieldMapping = {
    name: Array<MatchedFieldName>;
    title: Array<MatchedFieldName>;
    description: Array<MatchedFieldName>;
    fieldDescription: Array<MatchedFieldName>;
    tags: Array<MatchedFieldName>;
    fieldTags: Array<MatchedFieldName>;
    terms: Array<MatchedFieldName>;
    fieldTerms: Array<MatchedFieldName>;
    fieldPaths: Array<MatchedFieldName>;
    fieldLabels: Array<MatchedFieldName>;
};

export type NormalizedMatchedFieldName = keyof MatchFieldMapping;

export type MatchesGroupedByFieldName = {
    fieldName: string;
    matchedFields: Array<MatchedField>;
};

const DEFAULT_FIELD_MAPPING: MatchFieldMapping = {
    name: ['qualifiedName', 'displayName', 'name'],
    title: [],
    description: ['editedDescription', 'description'],
    fieldDescription: ['editedFieldDescriptions', 'fieldDescriptions'],
    tags: ['tags'],
    fieldTags: ['editedFieldTags', 'fieldTags'],
    terms: ['glossaryTerms'],
    fieldTerms: ['editedFieldGlossaryTerms'],
    fieldPaths: ['fieldPaths'],
    fieldLabels: ['fieldLabels'],
};

export const CORP_USER_FIELD_MAPPING: MatchFieldMapping = {
    ...DEFAULT_FIELD_MAPPING,
    title: ['title'],
};

export const CHART_DASHBOARD_FIELD_MAPPING: MatchFieldMapping = {
    ...DEFAULT_FIELD_MAPPING,
    name: [...DEFAULT_FIELD_MAPPING.name, 'title'],
};

export const MATCHED_FIELD_MAPPING = {
    [EntityType.CorpUser]: CORP_USER_FIELD_MAPPING,
    [EntityType.Chart]: CHART_DASHBOARD_FIELD_MAPPING,
    [EntityType.Dashboard]: CHART_DASHBOARD_FIELD_MAPPING,
    DEFAULT: DEFAULT_FIELD_MAPPING,
} as const;
