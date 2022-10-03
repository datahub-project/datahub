export const FILTER_URL_PREFIX = 'filter_';
export const SEARCH_FOR_ENTITY_PREFIX = 'SEARCH__';
export const EXACT_SEARCH_PREFIX = 'EXACT__';

export const ENTITY_FILTER_NAME = 'entity';
export const TAG_FILTER_NAME = 'tags';
export const GLOSSARY_FILTER_NAME = 'glossaryTerms';
export const CONTAINER_FILTER_NAME = 'container';
export const DOMAINS_FILTER_NAME = 'domains';
export const OWNERS_FILTER_NAME = 'owners';
export const TYPE_FILTER_NAME = 'typeNames';
export const PLATFORM_FILTER_NAME = 'platform';

export const FILTERS_TO_TRUNCATE = [
    TAG_FILTER_NAME,
    GLOSSARY_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    TYPE_FILTER_NAME,
    PLATFORM_FILTER_NAME,
];
export const TRUNCATED_FILTER_LENGTH = 5;

export const FIELD_TO_LABEL = {
    owners: 'Owner',
    tags: 'Tag',
    domains: 'Domain',
    platform: 'Platform',
    fieldTags: 'Column Tag',
    glossaryTerms: 'Term',
    fieldGlossaryTerms: 'Column Term',
    fieldPaths: 'Column Name',
    description: 'Description',
    fieldDescriptions: 'Column Description',
    removed: 'Soft Deleted',
    entity: 'Entity Type',
    container: 'Container',
    typeNames: 'Subtype',
    origin: 'Environment',
};

export const FIELDS_THAT_USE_CONTAINS_OPERATOR = ['description', 'fieldDescriptions'];

export const ADVANCED_SEARCH_ONLY_FILTERS = [
    'fieldGlossaryTerms',
    'fieldTags',
    'fieldPaths',
    'description',
    'fieldDescriptions',
    'removed',
];

export const DEGREE_FILTER = 'degree';

export enum UnionType {
    AND,
    OR,
}
