import i18next from 'i18next';

import { EntityType } from '@types';

export const FILTER_URL_PREFIX = 'filter_';
export const SEARCH_FOR_ENTITY_PREFIX = 'SEARCH__';
export const EXACT_SEARCH_PREFIX = 'EXACT__';

export const ENTITY_FILTER_NAME = '_entityType';
export const LEGACY_ENTITY_FILTER_NAME = 'entity';
export const ENTITY_INDEX_FILTER_NAME = '_index';
export const ENTITY_SUB_TYPE_FILTER_NAME = '_entityType␞typeNames';
export const TAGS_FILTER_NAME = 'tags';
export const PROPOSED_TAGS_FILTER_NAME = 'proposedTags';
export const PROPOSED_SCHEMA_TAGS_FILTER_NAME = 'proposedSchemaTags';
export const GLOSSARY_TERMS_FILTER_NAME = 'glossaryTerms';
export const PROPOSED_GLOSSARY_TERMS_FILTER_NAME = 'proposedGlossaryTerms';
export const PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME = 'proposedSchemaGlossaryTerms';
export const CONTAINER_FILTER_NAME = 'container';
export const DOMAINS_FILTER_NAME = 'domains';
export const DATA_PRODUCT_FILTER_NAME = 'dataProduct';
export const OWNERS_FILTER_NAME = 'owners';
export const TYPE_NAMES_FILTER_NAME = 'typeNames';
export const PLATFORM_FILTER_NAME = 'platform';
export const DATA_PLATFORM_INSTANCE_FILTER_NAME = 'platformInstance';
export const FIELD_TAGS_FILTER_NAME = 'fieldTags';
const EDITED_FIELD_TAGS_FILTER_NAME = 'editedFieldTags';
export const FIELD_GLOSSARY_TERMS_FILTER_NAME = 'fieldGlossaryTerms';
const EDITED_FIELD_GLOSSARY_TERMS_FILTER_NAME = 'editedFieldGlossaryTerms';
export const FIELD_PATHS_FILTER_NAME = 'fieldPaths';
export const FIELD_DESCRIPTIONS_FILTER_NAME = 'fieldDescriptions';
const EDITED_FIELD_DESCRIPTIONS_FILTER_NAME = 'editedFieldDescriptions';
export const DESCRIPTION_FILTER_NAME = 'description';
export const REMOVED_FILTER_NAME = 'removed';
export const ORIGIN_FILTER_NAME = 'origin';
export const DEGREE_FILTER_NAME = 'degree';
export const BROWSE_PATH_V2_FILTER_NAME = 'browsePathV2';
export const HAS_ACTIVE_INCIDENTS_FILTER_NAME = 'hasActiveIncidents';
export const HAS_FAILING_ASSERTIONS_FILTER_NAME = 'hasFailingAssertions';
export const HAS_SIBLINGS_FILTER_NAME = 'hasSiblings';
export const CHART_TYPE_FILTER_NAME = 'type';
export const LAST_MODIFIED_FILTER_NAME = 'lastModifiedAt';
export const STRUCTURED_PROPERTIES_FILTER_NAME = 'structuredProperties.';
export const SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME = 'showInColumnsTable';
export const SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME = 'showInAssetSummary';
export const COMPLETED_FORMS_FILTER_NAME = 'completedForms';
export const INCOMPLETE_FORMS_FILTER_NAME = 'incompleteForms';
export const VERIFIED_FORMS_FILTER_NAME = 'verifiedForms';
export const COMPLETED_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME = 'completedFormsCompletedPromptIds';
export const INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME = 'incompleteFormsCompletedPromptIds';
export const CREATED_TIME_FIELD_NAME = 'createdTime';

export const FILTER_DELIMITER = '␞';

export const ENTITY_SUB_TYPE_FILTER_FIELDS = [
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
];

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
];

// Getters, not plain values: each property re-evaluates i18next.t() on every read, so a
// label picks up the current language on its next render instead of freezing at module-init time.
export const FIELD_TO_LABEL: Record<string, string> = {
    get owners() {
        return i18next.t('search:fieldLabel.owner');
    },
    get tags() {
        return i18next.t('search:fieldLabel.tag');
    },
    get domains() {
        return i18next.t('search:fieldLabel.domain');
    },
    get dataProduct() {
        return i18next.t('search:fieldLabel.dataProduct');
    },
    get platform() {
        return i18next.t('search:fieldLabel.platform');
    },
    get fieldTags() {
        return i18next.t('search:fieldLabel.columnTag');
    },
    get glossaryTerms() {
        return i18next.t('search:fieldLabel.glossaryTerm');
    },
    get fieldGlossaryTerms() {
        return i18next.t('search:fieldLabel.columnTerm');
    },
    get fieldPaths() {
        return i18next.t('search:fieldLabel.columnName');
    },
    get description() {
        return i18next.t('common.labels:description');
    },
    get fieldDescriptions() {
        return i18next.t('search:fieldLabel.columnDescription');
    },
    get removed() {
        return i18next.t('search:fieldLabel.softDeleted');
    },
    get entity() {
        return i18next.t('search:fieldLabel.entityType');
    },
    get entityType() {
        return i18next.t('search:fieldLabel.entityType');
    },
    get _entityType() {
        return i18next.t('search:fieldLabel.entityType');
    },
    get container() {
        return i18next.t('search:fieldLabel.container');
    },
    get typeNames() {
        return i18next.t('search:fieldLabel.subType');
    },
    get origin() {
        return i18next.t('search:fieldLabel.environment');
    },
    get degree() {
        return i18next.t('search:fieldLabel.degree');
    },
    get '_entityType␞typeNames'() {
        return i18next.t('common.labels:type');
    },
    get platformInstance() {
        return i18next.t('search:fieldLabel.platformInstance');
    },
    get hasActiveIncidents() {
        return i18next.t('search:filters.incidents.hasActiveLabel');
    },
    get hasFailingAssertions() {
        return i18next.t('search:filters.assertions.hasFailingLabel');
    },
    get hasSiblings() {
        return i18next.t('search:filters.siblings.hasSiblingsLabel');
    },
    get [BROWSE_PATH_V2_FILTER_NAME]() {
        return i18next.t('search:fieldLabel.path');
    },
    get [LAST_MODIFIED_FILTER_NAME]() {
        return i18next.t('search:sort.lastModifiedInSource');
    },
    get [STRUCTURED_PROPERTIES_FILTER_NAME]() {
        return i18next.t('search:fieldLabel.structuredProperty');
    },
};

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

export const ENTITY_TYPE_FIELDS = new Set([ENTITY_SUB_TYPE_FILTER_NAME]);

export const BOOLEAN_FIELDS = new Set([
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
    REMOVED_FILTER_NAME,
]);

export const TEXT_FIELDS = new Set([DESCRIPTION_FILTER_NAME, FIELD_DESCRIPTIONS_FILTER_NAME, FIELD_PATHS_FILTER_NAME]);

export const ENTITY_FIELDS = new Set([
    CONTAINER_FILTER_NAME,
    TAGS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_TAGS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    DATA_PRODUCT_FILTER_NAME,
]);

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

export const ASSET_ENTITY_TYPES = [
    EntityType.Dataset,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.MlprimaryKey,
    EntityType.Container,
];

export const MIN_CHARACTER_COUNT_FOR_SEARCH = 3;

export const SEARCH_BAR_CLASS_NAME = 'search-bar';
