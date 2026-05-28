// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/vitest';
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import enCommonActions from '@src/i18n/locales/en/common.actions.json';
import enEntityIdentity from '@src/i18n/locales/en/entity.identity.json';
import enEntityOwnership from '@src/i18n/locales/en/entity.ownership.json';
import enEntityViews from '@src/i18n/locales/en/entity.views.json';
import enHomeV2 from '@src/i18n/locales/en/home.v2.json';
import enHomeV3 from '@src/i18n/locales/en/home.v3.json';
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
        'entity.identity',
        'entity.ownership',
        'entity.views',
        'home.v2',
        'home.v3',
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
            'entity.identity': enEntityIdentity,
            'entity.ownership': enEntityOwnership,
            'entity.views': enEntityViews,
            'home.v2': enHomeV2,
            'home.v3': enHomeV3,
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
