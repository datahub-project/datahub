// Recorded boot responses for the signed-in shell. The lab has no GMS.
// Missing operations return an empty data object so Apollo does not treat the
// stub as a network failure and bounce the session back to /login.

const ACTOR = 'urn:li:corpuser:boot';

const PRIVILEGES = [
    'viewAnalytics',
    'managePolicies',
    'manageIdentities',
    'generatePersonalAccessTokens',
    'manageIngestion',
    'manageSecrets',
    'manageTokens',
    'manageDomains',
    'viewTests',
    'manageTests',
    'manageGlossaries',
    'manageUserCredentials',
    'manageTags',
    'viewManageTags',
    'createDomains',
    'createTags',
    'createLogicalModels',
    'manageGlobalViews',
    'manageGlobalSettings',
    'manageOwnershipTypes',
    'manageGlobalAnnouncements',
    'createBusinessAttributes',
    'manageBusinessAttributes',
    'manageStructuredProperties',
    'viewStructuredPropertiesPage',
    'manageApplications',
    'manageFeatures',
    'manageHomePageTemplates',
    'manageDocuments',
    'createDocuments',
    'manageServiceAccounts',
];

function platformPrivileges() {
    return Object.fromEntries(PRIVILEGES.map((key) => [key, false]));
}

function appConfig() {
    return {
        appVersion: 'boot-only',
        policiesConfig: { enabled: false, platformPrivileges: [], resourcePrivileges: [] },
        analyticsConfig: { enabled: false },
        authConfig: {
            tokenAuthEnabled: false,
            allowNoExpiry: false,
            allowedAccessTokenDurations: [],
        },
        identityManagementConfig: { enabled: false },
        lineageConfig: { supportsImpactAnalysis: false },
        managedIngestionConfig: { enabled: false },
        visualConfig: {
            logoUrl: null,
            faviconUrl: null,
            appTitle: null,
            showFullTitleInLineage: false,
            queriesTab: { queriesTabResultSize: 5 },
            entityProfiles: { domain: { defaultTab: null } },
            searchResult: { enableNameHighlight: false },
            theme: { themeId: null },
            application: { showSidebarSectionWhenEmpty: false, showApplicationInNavigation: false },
        },
        telemetryConfig: { enableThirdPartyLogging: false },
        testsConfig: { enabled: false },
        viewsConfig: { enabled: false },
        searchBarConfig: { apiVariant: 'AUTOCOMPLETE_FOR_MULTIPLE' },
        searchCardConfig: { showDescription: false },
        searchFlagsConfig: { defaultSkipHighlighting: false },
        homePageConfig: { firstInPersonalSidebar: 'YOUR_ASSETS' },
        featureFlags: {
            readOnlyModeEnabled: false,
            showSearchFiltersV2: true,
            showBrowseV2: true,
            showAcrylInfo: false,
            erModelRelationshipFeatureEnabled: false,
            showAccessManagement: false,
            nestedDomainsEnabled: true,
            platformBrowseV2: false,
            businessAttributeEntityEnabled: false,
            contextDocumentsEnabled: false,
            documentExplicitSaveEnabled: false,
            dataContractsEnabled: false,
            editableDatasetNameEnabled: false,
            themeV2Enabled: true,
            themeV2Default: true,
            themeV2Toggleable: false,
            themeDarkModeEnabled: false,
            showSeparateSiblings: false,
            showManageStructuredProperties: false,
            schemaFieldCLLEnabled: false,
            hideDbtSourceInLineage: false,
            schemaFieldLineageIgnoreStatus: false,
            showNavBarRedesign: true,
            showAutoCompleteResults: false,
            entityVersioningEnabled: false,
            showHasSiblingsFilter: false,
            showSearchBarAutocompleteRedesign: false,
            showManageTags: false,
            showIntroducePage: false,
            showIngestionPageRedesign: false,
            showLineageExpandMore: false,
            showLineageFilterNodes: false,
            showStatsTabRedesign: false,
            showHomePageRedesign: true,
            showDefaultExternalLinks: true,
            showProductUpdates: false,
            logicalModelsEnabled: false,
            showHomepageUserRole: false,
            assetSummaryPageV1: false,
            datasetSummaryPageV1: false,
            metricsEnabled: false,
            ingestionOnboardingRedesignV1: false,
            documentationFileUploadV1: false,
            hideLineageInSearchCards: false,
            dataProductLineageEnabled: false,
            glossaryBasedPoliciesEnabled: false,
            structuredPropertiesInPoliciesEnabled: false,
            multipleDataProductsPerAsset: false,
            showTestsInHealthIcon: false,
            i18nEnabled: false,
            browserTracingEnabled: false,
            browserWebVitalsEnabled: false,
        },
        chromeExtensionConfig: { enabled: false, lineageEnabled: false },
    };
}

function getMe() {
    return {
        me: {
            corpUser: {
                type: 'CORP_USER',
                urn: ACTOR,
                username: 'boot',
                info: {
                    active: true,
                    displayName: 'Boot User',
                    title: null,
                    firstName: 'Boot',
                    lastName: 'User',
                    fullName: 'Boot User',
                    email: 'boot@example.com',
                },
                editableProperties: {
                    displayName: 'Boot User',
                    title: null,
                    pictureLink: null,
                    teams: [],
                    skills: [],
                    persona: null,
                    platforms: [],
                },
                settings: {
                    appearance: { showSimplifiedHomepage: false },
                    views: { defaultView: null },
                    homePage: { pageTemplate: null, dismissedAnnouncementUrns: [] },
                    locale: { language: 'en' },
                },
            },
            platformPrivileges: platformPrivileges(),
        },
    };
}

const FIXTURES = {
    appConfig: () => ({ appConfig: appConfig() }),
    getPolicyPrivileges: () => ({ appConfig: appConfig() }),
    getMe,
    getGlobalViewsSettings: () => ({ globalViewsSettings: { defaultView: null } }),
    getHomePageSettings: () => ({ globalHomePageSettings: { defaultTemplate: null } }),
    getLatestProductUpdate: () => ({ latestProductUpdate: null }),
    getDocPropagationSettings: () => ({ docPropagationSettings: { docColumnPropagation: false } }),
    getEntityCounts: () => ({ getEntityCounts: { counts: [] } }),
};

export function hasGraphqlFixture(operationName) {
    return Object.hasOwn(FIXTURES, operationName);
}

export function graphqlStubData(operationName) {
    const build = FIXTURES[operationName];
    return build ? build() : {};
}
