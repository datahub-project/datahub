import { EntityType } from '@types';

export const EMPTY_MESSAGES = {
    documentation: {
        title: 'No documentation yet',
    },
    tags: {
        title: 'No tags yet',
        description: 'Tag entities to help make them more discoverable and call out their most important attributes.',
    },
    terms: {
        title: 'No terms yet',
        description: 'Apply glossary terms to entities to classify their data.',
    },
    owners: {
        title: 'No owners yet',
        description: 'Adding owners helps you keep track of who is responsible for this data.',
    },
    properties: {
        title: 'No properties',
        description: 'Properties will appear here if they exist in your data source.',
    },
    queries: {
        title: 'No queries yet',
        description: 'Create, view, and share commonly used queries for this dataset.',
    },
    domain: {
        title: 'No domain yet',
        description: 'Group related entities based on your organizational structure using by adding them to a Domain.',
    },
    dataProduct: {
        title: 'No product yet',
        description: 'Group related entities based on shared characteristics by adding them to a Data Product.',
    },
    application: {
        title: 'No application yet',
        description: 'Associate entities with applications to track ownership and lifecycle.',
    },
    contains: {
        title: 'Does not Contain any Glossary Terms',
        description: 'Terms can contain other terms to represent a "Has A" style relationship.',
    },
    inherits: {
        title: 'Does not Inherit any Glossary Terms',
        description: 'Terms can inherit from other terms to represent an "Is A" style relationship.',
    },
    'contained by': {
        title: 'Is not Contained by any Glossary Terms',
        description: 'Terms can be contained by other terms to represent a "Has A" style relationship.',
    },
    'inherited by': {
        title: 'Is not Inherited by any Glossary Terms',
        description: 'Terms can be inherited by other terms to represent an "Is A" style relationship.',
    },
    structuredProps: {
        title: 'No value set',
    },
};

export const ELASTIC_MAX_COUNT = 10000;

export const getElasticCappedTotalValueText = (count: number) => {
    if (count === ELASTIC_MAX_COUNT) {
        return `${ELASTIC_MAX_COUNT}+`;
    }

    return `${count}`;
};

export const ENTITY_TYPES_WITH_MANUAL_LINEAGE = new Set([
    EntityType.Dashboard,
    EntityType.Chart,
    EntityType.Dataset,
    EntityType.DataJob,
]);

export const GLOSSARY_ENTITY_TYPES = [EntityType.GlossaryTerm, EntityType.GlossaryNode];

export const DEFAULT_SYSTEM_ACTOR_URNS = ['urn:li:corpuser:__datahub_system', 'urn:li:corpuser:unknown'];

export const VIEW_ENTITY_PAGE = 'VIEW_ENTITY_PAGE';

// only values for Domain Entity for custom configurable default tab
export enum EntityProfileTab {
    DOMAIN_ENTITIES_TAB = 'DOMAIN_ENTITIES_TAB',
    DOCUMENTATION_TAB = 'DOCUMENTATION_TAB',
    DATA_PRODUCTS_TAB = 'DATA_PRODUCTS_TAB',
    SUMMARY_TAB = 'SUMMARY_TAB',
}

export const EDITING_DOCUMENTATION_URL_PARAM = 'editing';

export const UNKNOWN_DATA_PLATFORM = 'urn:li:dataPlatform:unknown';

export const SMART_ASSERTION_STALE_IN_DAYS = 3;

export const TITLE_CASE_EXCEPTION_WORDS = ['of', 'the', 'in', 'on', 'and', 'a', 'an', 'to', 'for', 'at', 'by'];

export const RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES = 'RecentlyViewedEntities';
export const RECOMMENDATION_MODULE_ID_RECENTLY_EDITED_ENTITIES = 'RecentlyEditedEntities';
export const RECOMMENDATION_MODULE_ID_RECENT_SEARCHES = 'RecentSearches';

export const ENTITY_TYPES_WITH_NEW_SUMMARY_TAB = [
    EntityType.GlossaryNode,
    EntityType.GlossaryTerm,
    EntityType.DataProduct,
    EntityType.Domain,
];
