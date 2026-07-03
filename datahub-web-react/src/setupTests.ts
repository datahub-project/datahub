// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/vitest';
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import enAlchemy from '@src/i18n/locales/en/alchemy.json';
import enAnalytics from '@src/i18n/locales/en/analytics.json';
import enAuth from '@src/i18n/locales/en/auth.json';
import enCommonActions from '@src/i18n/locales/en/common.actions.json';
import enCommonCounts from '@src/i18n/locales/en/common.counts.json';
import enCommonFeedback from '@src/i18n/locales/en/common.feedback.json';
import enCommonLabels from '@src/i18n/locales/en/common.labels.json';
import enEntityForm from '@src/i18n/locales/en/entity.form.json';
import enEntityIdentity from '@src/i18n/locales/en/entity.identity.json';
import enEntityOwnership from '@src/i18n/locales/en/entity.ownership.json';
import enEntityPreview from '@src/i18n/locales/en/entity.preview.json';
import enEntityProfileAccess from '@src/i18n/locales/en/entity.profile.access.json';
import enEntityProfileDocumentation from '@src/i18n/locales/en/entity.profile.documentation.json';
import enEntityProfileEditor from '@src/i18n/locales/en/entity.profile.editor.json';
import enEntityProfileIncident from '@src/i18n/locales/en/entity.profile.incident.json';
import enEntityProfileQueries from '@src/i18n/locales/en/entity.profile.queries.json';
import enEntityProfileSchema from '@src/i18n/locales/en/entity.profile.schema.json';
import enEntityProfileStats from '@src/i18n/locales/en/entity.profile.stats.json';
import enEntityProfileSummary from '@src/i18n/locales/en/entity.profile.summary.json';
import enEntityProfileTabs from '@src/i18n/locales/en/entity.profile.tabs.json';
import enEntityProfileTests from '@src/i18n/locales/en/entity.profile.tests.json';
import enEntityProfileTimeline from '@src/i18n/locales/en/entity.profile.timeline.json';
import enEntityProfileValidations from '@src/i18n/locales/en/entity.profile.validations.json';
import enEntityProfileView from '@src/i18n/locales/en/entity.profile.view.json';
import enEntitySharedActions from '@src/i18n/locales/en/entity.shared.actions.json';
import enEntitySharedComponents from '@src/i18n/locales/en/entity.shared.components.json';
import enEntitySharedContainers from '@src/i18n/locales/en/entity.shared.containers.json';
import enEntitySharedEmptyStates from '@src/i18n/locales/en/entity.shared.emptyStates.json';
import enEntitySharedEntityDropdown from '@src/i18n/locales/en/entity.shared.entityDropdown.json';
import enEntitySharedProfile from '@src/i18n/locales/en/entity.shared.profile.json';
import enEntitySharedSelectors from '@src/i18n/locales/en/entity.shared.selectors.json';
import enEntitySharedStats from '@src/i18n/locales/en/entity.shared.stats.json';
import enEntitySharedVersioning from '@src/i18n/locales/en/entity.shared.versioning.json';
import enEntityTypes from '@src/i18n/locales/en/entity.types.json';
import enEntityViews from '@src/i18n/locales/en/entity.views.json';
import enEntityV1SharedComponents from '@src/i18n/locales/en/entityV1.shared.components.json';
import enGovernanceDomain from '@src/i18n/locales/en/governance.domain.json';
import enGovernanceGlossary from '@src/i18n/locales/en/governance.glossary.json';
import enGovernanceStructuredProperties from '@src/i18n/locales/en/governance.structured-properties.json';
import enHomeV2 from '@src/i18n/locales/en/home.v2.json';
import enHomeV3 from '@src/i18n/locales/en/home.v3.json';
import enIngestSources from '@src/i18n/locales/en/ingest.sources.json';
import enIngestion from '@src/i18n/locales/en/ingestion.json';
import enIngestionSourceBuilder from '@src/i18n/locales/en/ingestion.sourceBuilder.json';
import enLineage from '@src/i18n/locales/en/lineage.json';
import enMisc from '@src/i18n/locales/en/misc.json';
import enModules from '@src/i18n/locales/en/modules.json';
import enOnboarding from '@src/i18n/locales/en/onboarding.json';
import enSearch from '@src/i18n/locales/en/search.json';
import enSettingsFeatures from '@src/i18n/locales/en/settings.features.json';
import enSettingsPage from '@src/i18n/locales/en/settings.page.json';
import enSettingsPermissions from '@src/i18n/locales/en/settings.permissions.json';
import enSettingsPosts from '@src/i18n/locales/en/settings.posts.json';
import enSettingsPreferences from '@src/i18n/locales/en/settings.preferences.json';
import enSettingsTokens from '@src/i18n/locales/en/settings.tokens.json';
import enSharedBusinessAttribute from '@src/i18n/locales/en/shared.business-attribute.json';
import enSharedConfirmation from '@src/i18n/locales/en/shared.confirmation.json';
import enSharedError from '@src/i18n/locales/en/shared.error.json';
import enSharedHealth from '@src/i18n/locales/en/shared.health.json';
import enSharedMisc from '@src/i18n/locales/en/shared.misc.json';
import enSharedProduct from '@src/i18n/locales/en/shared.product.json';
import enSharedPropagation from '@src/i18n/locales/en/shared.propagation.json';
import enSharedQueryBuilder from '@src/i18n/locales/en/shared.query-builder.json';
import enSharedSearch from '@src/i18n/locales/en/shared.search.json';
import enSharedShare from '@src/i18n/locales/en/shared.share.json';
import enSharedTags from '@src/i18n/locales/en/shared.tags.json';
import enSharedTime from '@src/i18n/locales/en/shared.time.json';
import { NAMESPACES } from '@src/i18n/namespaces';
import '@utils/dayjs';

i18n.use(initReactI18next).init({
    lng: 'en',
    fallbackLng: 'en',
    initImmediate: false,
    ns: [...NAMESPACES],
    resources: {
        en: {
            alchemy: enAlchemy,
            analytics: enAnalytics,
            auth: enAuth,
            'common.actions': enCommonActions,
            'common.counts': enCommonCounts,
            'common.feedback': enCommonFeedback,
            'common.labels': enCommonLabels,
            'entity.form': enEntityForm,
            'entity.identity': enEntityIdentity,
            'entity.profile.documentation': enEntityProfileDocumentation,
            'entity.profile.editor': enEntityProfileEditor,
            'entity.ownership': enEntityOwnership,
            'entity.preview': enEntityPreview,
            'entity.profile.incident': enEntityProfileIncident,
            'entity.profile.tabs': enEntityProfileTabs,
            'entity.profile.tests': enEntityProfileTests,
            'entity.profile.validations': enEntityProfileValidations,
            'entity.profile.access': enEntityProfileAccess,
            'entity.profile.queries': enEntityProfileQueries,
            'entity.profile.schema': enEntityProfileSchema,
            'entity.profile.stats': enEntityProfileStats,
            'entity.profile.summary': enEntityProfileSummary,
            'entity.profile.timeline': enEntityProfileTimeline,
            'entity.profile.view': enEntityProfileView,
            'entity.shared.actions': enEntitySharedActions,
            'entity.shared.components': enEntitySharedComponents,
            'entity.shared.containers': enEntitySharedContainers,
            'entity.shared.emptyStates': enEntitySharedEmptyStates,
            'entity.shared.entityDropdown': enEntitySharedEntityDropdown,
            'entity.shared.profile': enEntitySharedProfile,
            'entity.shared.selectors': enEntitySharedSelectors,
            'entity.shared.stats': enEntitySharedStats,
            'entity.shared.versioning': enEntitySharedVersioning,
            'entity.types': enEntityTypes,
            'entity.views': enEntityViews,
            'entityV1.shared.components': enEntityV1SharedComponents,
            'governance.domain': enGovernanceDomain,
            'governance.glossary': enGovernanceGlossary,
            'governance.structured-properties': enGovernanceStructuredProperties,
            'home.v2': enHomeV2,
            'home.v3': enHomeV3,
            'ingest.sources': enIngestSources,
            ingestion: enIngestion,
            'ingestion.sourceBuilder': enIngestionSourceBuilder,
            lineage: enLineage,
            misc: enMisc,
            modules: enModules,
            onboarding: enOnboarding,
            'settings.features': enSettingsFeatures,
            'settings.page': enSettingsPage,
            'settings.permissions': enSettingsPermissions,
            'settings.posts': enSettingsPosts,
            'settings.preferences': enSettingsPreferences,
            'settings.tokens': enSettingsTokens,
            search: enSearch,
            'shared.business-attribute': enSharedBusinessAttribute,
            'shared.confirmation': enSharedConfirmation,
            'shared.error': enSharedError,
            'shared.health': enSharedHealth,
            'shared.misc': enSharedMisc,
            'shared.product': enSharedProduct,
            'shared.propagation': enSharedPropagation,
            'shared.query-builder': enSharedQueryBuilder,
            'shared.search': enSharedSearch,
            'shared.share': enSharedShare,
            'shared.tags': enSharedTags,
            'shared.time': enSharedTime,
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
