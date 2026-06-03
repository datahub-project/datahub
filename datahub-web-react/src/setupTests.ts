// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/vitest';
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import enCommonActions from '@src/i18n/locales/en/common.actions.json';
import enCommonFeedback from '@src/i18n/locales/en/common.feedback.json';
import enCommonLabels from '@src/i18n/locales/en/common.labels.json';
import enEntityIdentity from '@src/i18n/locales/en/entity.identity.json';
import enEntityOwnership from '@src/i18n/locales/en/entity.ownership.json';
import enEntityProfileAccess from '@src/i18n/locales/en/entity.profile.access.json';
import enEntityProfileDocumentation from '@src/i18n/locales/en/entity.profile.documentation.json';
import enEntityProfileIncident from '@src/i18n/locales/en/entity.profile.incident.json';
import enEntityProfileQueries from '@src/i18n/locales/en/entity.profile.queries.json';
import enEntityProfileSchema from '@src/i18n/locales/en/entity.profile.schema.json';
import enEntityProfileStats from '@src/i18n/locales/en/entity.profile.stats.json';
import enEntityProfileValidations from '@src/i18n/locales/en/entity.profile.validations.json';
import enEntityProfileView from '@src/i18n/locales/en/entity.profile.view.json';
import enEntityViews from '@src/i18n/locales/en/entity.views.json';
import enGovernanceDomain from '@src/i18n/locales/en/governance.domain.json';
import enGovernanceGlossary from '@src/i18n/locales/en/governance.glossary.json';
import enGovernanceStructuredProperties from '@src/i18n/locales/en/governance.structured-properties.json';
import enHomeV2 from '@src/i18n/locales/en/home.v2.json';
import enHomeV3 from '@src/i18n/locales/en/home.v3.json';
import enMisc from '@src/i18n/locales/en/misc.json';
import enModules from '@src/i18n/locales/en/modules.json';
import enSettingsFeatures from '@src/i18n/locales/en/settings.features.json';
import enSettingsPage from '@src/i18n/locales/en/settings.page.json';
import enSettingsPermissions from '@src/i18n/locales/en/settings.permissions.json';
import enSettingsPosts from '@src/i18n/locales/en/settings.posts.json';
import enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';
import enSettingsTokens from '@src/i18n/locales/en/settings.tokens.json';
import enSharedQueryBuilder from '@src/i18n/locales/en/shared.query-builder.json';
import '@utils/dayjs';

i18n.use(initReactI18next).init({
    lng: 'en',
    fallbackLng: 'en',
    initImmediate: false,
    ns: [
        'common.actions',
        'common.feedback',
        'common.labels',
        'entity.identity',
        'entity.profile.documentation',
        'entity.ownership',
        'entity.profile.incident',
        'entity.profile.validations',
        'entity.profile.access',
        'entity.profile.queries',
        'entity.profile.schema',
        'entity.profile.stats',
        'entity.profile.view',
        'entity.views',
        'governance.domain',
        'governance.glossary',
        'governance.structured-properties',
        'home.v2',
        'home.v3',
        'misc',
        'modules',
        'settings.features',
        'settings.page',
        'settings.permissions',
        'settings.posts',
        'settings.preferences',
        'settings.tokens',
        'shared.query-builder',
    ],
    resources: {
        en: {
            'common.actions': enCommonActions,
            'common.feedback': enCommonFeedback,
            'common.labels': enCommonLabels,
            'entity.identity': enEntityIdentity,
            'entity.profile.documentation': enEntityProfileDocumentation,
            'entity.ownership': enEntityOwnership,
            'entity.profile.incident': enEntityProfileIncident,
            'entity.profile.validations': enEntityProfileValidations,
            'entity.profile.access': enEntityProfileAccess,
            'entity.profile.queries': enEntityProfileQueries,
            'entity.profile.schema': enEntityProfileSchema,
            'entity.profile.stats': enEntityProfileStats,
            'entity.profile.view': enEntityProfileView,
            'entity.views': enEntityViews,
            'governance.domain': enGovernanceDomain,
            'governance.glossary': enGovernanceGlossary,
            'governance.structured-properties': enGovernanceStructuredProperties,
            'home.v2': enHomeV2,
            'home.v3': enHomeV3,
            misc: enMisc,
            modules: enModules,
            'settings.features': enSettingsFeatures,
            'settings.page': enSettingsPage,
            'settings.permissions': enSettingsPermissions,
            'settings.posts': enSettingsPosts,
            'settings.preferences': enSettingsPreferences,
            'settings.tokens': enSettingsTokens,
            'shared.query-builder': enSharedQueryBuilder,
        },
    },
    interpolation: { escapeValue: false },
});

// Mock window.matchMedia interface.
// See https://jestjs.io/docs/en/manual-mocks#mocking-methods-which-are-not-implemented-in-jsdom
// and https://github.com/ant-design/ant-design/issues/21096.
global.matchMedia =
    global.matchMedia ||
    (() => {
        return {
            matches: false,
            addListener: vi.fn(),
            removeListener: vi.fn(),
        };
    });

window.location = {
    ...window.location,
    replace: () => {},
};

// Suppress `Error: Not implemented: window.computedStyle(elt, pseudoElt)`.
// From https://github.com/vitest-dev/vitest/issues/2061
// and https://github.com/NickColley/jest-axe/issues/147#issuecomment-758804533
const { getComputedStyle } = window;
window.getComputedStyle = (elt) => getComputedStyle(elt);

vi.mock('js-cookie', () => ({
    default: {
        get: () => 'urn:li:corpuser:2',
    },
}));
vi.mock('./app/entity/shared/tabs/Documentation/components/editor/Editor');

vi.stubGlobal(
    'ResizeObserver',
    vi.fn(() => ({
        observe: vi.fn(),
        unobserve: vi.fn(),
        disconnect: vi.fn(),
    })),
);

vi.stubGlobal(
    'IntersectionObserver',
    vi.fn(() => ({
        observe: vi.fn(),
        unobserve: vi.fn(),
        disconnect: vi.fn(),
        root: null,
        rootMargin: '',
        thresholds: [],
        takeRecords: vi.fn(() => []),
    })),
);
