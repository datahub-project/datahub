import { EntityType } from '../../../types.generated';

// TODO(Gabe): integrate this w/ the theme
export const REDESIGN_COLORS = {
    GREY: '#e5e5e5',
    BLUE: '#1890FF',
    DARK_GREY: '#56668E',
    HEADING_COLOR: '#403D5C',
    LIGHT_GREY: '#F6F7FA',
    BACKGROUND_GREY: '#F5F5F5',
    PRIMARY_DARK_GREEN: '#113633',
    TERTIARY_GREEN: '#3CB47A',
    WHITE_WIRE: '#F1F1F1',
    WHITE: '#FFF',
    BLACK: '#000',
    SIDE_BAR: '#E8E6EB',
    PRIMARY_PURPLE: '#736BA4',
    BACKGROUND_PURPLE: '#8D76E9',
    TITLE_PURPLE: '#533FD1',
    HOVER_PURPLE: '#3e2f9d',
    PLACEHOLDER_PURPLE: '#9AA4BB',
    LINK_HOVER_BLUE: '#5280E2',
    RED_ERROR_BORDER: '#FFA39E',
    BACKGROUND_SECONDARY_GRAY: '#AAA2CB0F',
    BACKGROUND_GRAY_2: '#FAF9FC',
    BACKGROUND_GRAY_3: '#F6F7FA',
    BACKGROUND_OVERLAY_BLACK: '#171723',
    BACKGROUND_OVERLAY_BLACK_SEARCH: '#404053',
    BACKGROUND_PRIMARY_1: '#533fd1',
    VIEW_PURPLE: '#9178f6',
    BORDER_1: '#4b4b54',
    BORDER_2: '#E6E6E6',
    BORDER_3: '#EFEFEF',
    BORDER_4: '#533FD1',
    SECONDARY_LIGHT_GREY: '#9DA7C0',
    ACTION_ICON_GREY: '#676b75',
    AVATAR_STYLE_WHITE_BACKGROUND: '#ffffff66',
    GROUP_AVATAR_STYLE_GRADIENT: 'linear-gradient(0deg, #CB427B 0%, #CB427B 100%), #65B5C0',
    PROFILE_AVATAR_STYLE_GRADIENT: 'linear-gradient(93deg, #23c5b1 5.11%, #30d572 112.87%), #65b5c0',
    SIDE_BAR_BORDER_RIGHT: '#e8e8e8',
    DARK_PURPLE: '#6C6B88',
    LINK_GREY: '#586287',
    TEXT_GREY: '#8D95B1',
    WARNING_RED: '#d07b7b',
    SUBTITLE: '#434863',
    LIGHT_GREY_BORDER: '#ededed',
    BACKGROUND_PURPLE_2: '#887fae',
};

export const SEARCH_COLORS = {
    TITLE_PURPLE: '#533FD1',
    SUBTEXT_PURPPLE: '#3F54D1',
    BACKGROUND_PURPLE: '#ece9f8',
    PLATFORM_TEXT: '#56668E',
    MATCH_BACKGROUND_GREY: '#5A617110',
    MATCH_TEXT_GREY: '#8894A9',
    LINK_BLUE: '#5280E2',
};

export const LINEAGE_COLORS = {
    BLUE_1: '#0958D9',
    BLUE_2: '#1890FF',
    PURPLE_1: '#5280E2',
    PURPLE_2: '#324473',
    PURPLE_3: SEARCH_COLORS.TITLE_PURPLE,
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
    10: '#272727',
    11: '#262626',
};

export const ANTD_GRAY_V2 = {
    1: '#F8F9Fa',
    2: '#F3F5F6',
    5: '#DDE0E4',
    6: '#B2B8BD',
    8: '#5E666E',
    10: '#1B1E22',
    11: '#6C6B88',
    12: '#52596c',
    13: '#ababab',
    14: '#f7f7f7',
};

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

export const THEME_COLOR_BLUE = '#328980';
