import { EntityType } from '../../../types.generated';

// TODO(Gabe): integrate this w/ the theme
export const REDESIGN_COLORS = {
    GREY: '#e5e5e5',
    BLUE: '#1890FF',
};

export const ANTD_GRAY = {
    1: '#FFFFFF',
    2: '#FAFAFA',
    2.5: '#F8F8F8',
    3: '#F5F5F5',
    4: '#F0F0F0',
    4.5: '#E9E9E9',
    5: '#D9D9D9',
    6: '#BFBFBF',
    7: '#8C8C8C',
    8: '#595959',
    9: '#434343',
};

export const ANTD_GRAY_V2 = {
    1: '#F8F9Fa',
    2: '#F3F5F6',
    5: '#DDE0E4',
    6: '#B2B8BD',
    8: '#5E666E',
    10: '#1B1E22',
};

export const EMPTY_MESSAGES = {
    documentation: {
        title: 'No documentation yet',
        description: 'Share your knowledge by adding documentation and links to helpful resources.',
    },
    tags: {
        title: 'No tags added yet',
        description: 'Tag entities to help make them more discoverable and call out their most important attributes.',
    },
    terms: {
        title: 'No terms added yet',
        description: 'Apply glossary terms to entities to classify their data.',
    },
    owners: {
        title: 'No owners added yet',
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
        title: 'No domain set',
        description: 'Group related entities based on your organizational structure using by adding them to a Domain.',
    },
    dataProduct: {
        title: 'No data product set',
        description: 'Group related entities based on shared characteristics by adding them to a Data Product.',
    },
    contains: {
        title: 'Contains no Terms',
        description: 'Terms can contain other terms to represent a "Has A" style relationship.',
    },
    inherits: {
        title: 'Does not inherit from any terms',
        description: 'Terms can inherit from other terms to represent an "Is A" style relationship.',
    },
    'contained by': {
        title: 'Is not contained by any terms',
        description: 'Terms can be contained by other terms to represent a "Has A" style relationship.',
    },
    'inherited by': {
        title: 'Is not inherited by any terms',
        description: 'Terms can be inherited by other terms to represent an "Is A" style relationship.',
    },
    businessAttributes: {
        title: 'No business attributes added yet',
        description: 'Add business attributes to entities to classify their data.',
    },
    mlModel: {
        title: 'No ML models',
        description: 'ML models will appear here if they are associated with this ML model group.',
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
}
