import { EntityType } from '@types';

export const FILTER_URL_PREFIX = 'filter_';
export const SEARCH_FOR_ENTITY_PREFIX = 'SEARCH__';
export const EXACT_SEARCH_PREFIX = 'EXACT__';

export const ENTITY_FILTER_NAME = '_entityType';
export const LEGACY_ENTITY_FILTER_NAME = 'entity';
export const ENTITY_INDEX_FILTER_NAME = '_index';
export const ENTITY_SUB_TYPE_FILTER_NAME = '_entityType␞typeNames';
export const TAGS_FILTER_NAME = 'tags';
export const GLOSSARY_TERMS_FILTER_NAME = 'glossaryTerms';
export const CONTAINER_FILTER_NAME = 'container';
export const DOMAINS_FILTER_NAME = 'domains';
export const OWNERS_FILTER_NAME = 'owners';
export const TYPE_NAMES_FILTER_NAME = 'typeNames';
export const PLATFORM_FILTER_NAME = 'platform';
export const FIELD_TAGS_FILTER_NAME = 'fieldTags';
export const EDITED_FIELD_TAGS_FILTER_NAME = 'editedFieldTags';
export const FIELD_GLOSSARY_TERMS_FILTER_NAME = 'fieldGlossaryTerms';
export const EDITED_FIELD_GLOSSARY_TERMS_FILTER_NAME = 'editedFieldGlossaryTerms';
export const FIELD_PATHS_FILTER_NAME = 'fieldPaths';
export const FIELD_DESCRIPTIONS_FILTER_NAME = 'fieldDescriptions';
export const EDITED_FIELD_DESCRIPTIONS_FILTER_NAME = 'editedFieldDescriptions';
export const DESCRIPTION_FILTER_NAME = 'description';
export const REMOVED_FILTER_NAME = 'removed';
export const ORIGIN_FILTER_NAME = 'origin';
export const DEGREE_FILTER_NAME = 'degree';
export const BROWSE_PATH_V2_FILTER_NAME = 'browsePathV2';
export const STRUCTURED_PROPERTIES_FILTER_NAME = 'structuredProperties.';
export const ENTITY_TYPES_FILTER_NAME = 'entityTypes';
export const IS_HIDDEN_PROPERTY_FILTER_NAME = 'isHidden';
export const SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME = 'showInColumnsTable';
export const SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME = 'showInAssetSummary';
export const HAS_ACTIVE_INCIDENTS_FILTER_NAME = 'hasActiveIncidents';
export const HAS_FAILING_ASSERTIONS_FILTER_NAME = 'hasFailingAssertions';
export const OUTPUT_PORTS_FIELD = 'isOutputPort';
export const COMPLETED_FORMS_FILTER_NAME = 'completedForms';
export const INCOMPLETE_FORMS_FILTER_NAME = 'incompleteForms';
export const VERIFIED_FORMS_FILTER_NAME = 'verifiedForms';
export const COMPLETED_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME = 'completedFormsCompletedPromptIds';
export const INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME = 'incompleteFormsCompletedPromptIds';

export const LEGACY_ENTITY_FILTER_FIELDS = [ENTITY_FILTER_NAME, LEGACY_ENTITY_FILTER_NAME];

export const FILTER_DELIMITER = '␞';

export const ENTITY_SUB_TYPE_FILTER_FIELDS = [
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
];

export const FILTERS_TO_TRUNCATE = [
    TAGS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    PLATFORM_FILTER_NAME,
];
export const TRUNCATED_FILTER_LENGTH = 5;

export const ORDERED_FIELDS = [
    ENTITY_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    DESCRIPTION_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    DEGREE_FILTER_NAME,
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
];

export const FIELD_TO_LABEL = {
    owners: 'Owner',
    tags: 'Tag',
    domains: 'Domain',
    platform: 'Platform',
    fieldTags: 'Column Tag',
    glossaryTerms: 'Glossary Term',
    fieldGlossaryTerms: 'Column Glossary Term',
    fieldPaths: 'Column Name',
    description: 'Description',
    fieldDescriptions: 'Column Description',
    removed: 'Soft Deleted',
    entity: 'Entity Type',
    entityType: 'Entity Type',
    _entityType: 'Entity Type',
    container: 'Container',
    typeNames: 'Sub Type',
    origin: 'Environment',
    degree: 'Degree',
    [BROWSE_PATH_V2_FILTER_NAME]: 'Browse',
    hasActiveIncidents: 'Incidents',
    hasFailingAssertions: 'Assertions',
};

export const FIELDS_THAT_USE_CONTAINS_OPERATOR = [
    DESCRIPTION_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
];

export const ADVANCED_SEARCH_ONLY_FILTERS = [
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    EDITED_FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    EDITED_FIELD_TAGS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    DESCRIPTION_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    EDITED_FIELD_DESCRIPTIONS_FILTER_NAME,
    REMOVED_FILTER_NAME,
];

export enum UnionType {
    AND,
    OR,
}

export const UNIT_SEPARATOR = '␟';

export const FilterModes = {
    BASIC: 'basic',
    ADVANCED: 'advanced',
} as const;

export type FilterMode = (typeof FilterModes)[keyof typeof FilterModes];

export const MAX_COUNT_VAL = 10000;

export const EMBEDDED_LIST_SEARCH_ENTITY_TYPES = [
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
    EntityType.Tag,
    EntityType.Role,
    EntityType.CorpUser,
    EntityType.CorpGroup,
    EntityType.Container,
    EntityType.Domain,
    EntityType.DataProduct,
    EntityType.Notebook,
    EntityType.BusinessAttribute,
    EntityType.DataProcessInstance,
];
