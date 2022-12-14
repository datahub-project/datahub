export const FILTER_URL_PREFIX = 'filter_';
export const SEARCH_FOR_ENTITY_PREFIX = 'SEARCH__';
export const EXACT_SEARCH_PREFIX = 'EXACT__';

export const ENTITY_FILTER_NAME = 'entity';
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
    container: 'Container',
    typeNames: 'Sub Type',
    origin: 'Environment',
    degree: 'Degree',
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
