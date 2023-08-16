import { SortOrder } from '../../../types.generated';

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
    // todo - implement me
    | 'fieldPaths';

type MatchFieldMapping = {
    name: Array<MatchedFieldName>;
    description: Array<MatchedFieldName>;
    fieldDescription: Array<MatchedFieldName>;
    tags: Array<MatchedFieldName>;
    fieldTags: Array<MatchedFieldName>;
    terms: Array<MatchedFieldName>;
    fieldTerms: Array<MatchedFieldName>;
    fieldPaths: Array<MatchedFieldName>;
};

export type NormalizedMatchedFieldName = keyof MatchFieldMapping;

export const MATCHED_FIELD_MAPPING: MatchFieldMapping = {
    name: ['qualifiedName', 'displayName', 'title', 'name'],
    description: ['editedDescription', 'description'],
    fieldDescription: ['editedFieldDescriptions', 'fieldDescriptions'],
    tags: ['tags'],
    fieldTags: ['editedFieldTags', 'fieldTags'],
    terms: ['glossaryTerms'],
    fieldTerms: ['editedFieldGlossaryTerms'],
    fieldPaths: ['fieldPaths'],
};
