import i18next from 'i18next';

import { EntityType } from '@types';

// These DEFINE the deprecated raw palettes that the color initiative is migrating away from, so
// hardcoded hex values here are intentional. Usages are caught via no-restricted-imports; the
// definitions must remain until all consumers migrate to semantic tokens.
/* eslint-disable rulesdir/no-hardcoded-colors */
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
/* eslint-enable rulesdir/no-hardcoded-colors */

export const EMPTY_MESSAGES = {
    documentation: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.documentation.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.documentation.description');
        },
    },
    tags: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.tags.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.tags.description');
        },
    },
    terms: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.terms.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.terms.description');
        },
    },
    owners: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.owners.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.owners.description');
        },
    },
    properties: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.properties.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.properties.description');
        },
    },
    queries: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.queries.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.queries.description');
        },
    },
    domain: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.domain.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.domain.description');
        },
    },
    dataProduct: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.dataProduct.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.dataProduct.description');
        },
    },
    contains: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.contains.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.contains.description');
        },
    },
    inherits: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.inherits.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.inherits.description');
        },
    },
    'contained by': {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.containedBy.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.containedBy.description');
        },
    },
    'inherited by': {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.inheritedBy.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.inheritedBy.description');
        },
    },
    businessAttributes: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.businessAttributes.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.businessAttributes.description');
        },
    },
    mlModel: {
        get title() {
            return i18next.t('entity.profile.tabs:emptyTab.mlModel.title');
        },
        get description() {
            return i18next.t('entity.profile.tabs:emptyTab.mlModel.description');
        },
    },
};

const ELASTIC_MAX_COUNT = 10000;

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
