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

// The signed-in home page: a user who owns a handful of assets across a
// warehouse, BI and an orchestrator, with domains, platforms, two announcements
// and onboarding already done. Names are generic placeholders.
const DAY_MS = 24 * 60 * 60 * 1000;
const RECORDED_AT = Date.UTC(2026, 9, 27, 15, 0, 0);

const PLATFORM_INFO = {
    snowflake: { displayName: 'Snowflake', type: 'RELATIONAL_DB', logo: 'assets/platforms/snowflakelogo.png' },
    looker: { displayName: 'Looker', type: 'OTHERS', logo: 'assets/platforms/lookerlogo.svg' },
    dbt: { displayName: 'dbt', type: 'OTHERS', logo: 'assets/platforms/dbtlogo.png' },
    airflow: { displayName: 'Airflow', type: 'OTHERS', logo: 'assets/platforms/airflowlogo.png' },
    kafka: { displayName: 'Kafka', type: 'MESSAGE_BROKER', logo: 'assets/platforms/kafkalogo.png' },
};

function platform(name) {
    const { displayName, type, logo } = PLATFORM_INFO[name];
    const props = { type, displayName, datasetNameDelimiter: '.', logoUrl: logo };
    return {
        __typename: 'DataPlatform',
        urn: `urn:li:dataPlatform:${name}`,
        type: 'DATA_PLATFORM',
        lastIngested: RECORDED_AT - DAY_MS,
        name,
        properties: { __typename: 'DataPlatformProperties', ...props, logical: false },
        displayName,
        info: { __typename: 'DataPlatformInfo', ...props },
    };
}

const DOMAINS = [
    {
        id: 'finance',
        name: 'Finance',
        description: 'Revenue, billing and forecasting data.',
        color: '#2F6FED',
        assets: 1284,
        products: 6,
    },
    {
        id: 'marketing',
        name: 'Marketing',
        description: 'Campaign, attribution and web analytics.',
        color: '#D4572A',
        assets: 742,
        products: 3,
    },
    {
        id: 'product',
        name: 'Product Analytics',
        description: 'Product usage events and derived metrics.',
        color: '#3A9D5D',
        assets: 2310,
        products: 9,
    },
    {
        id: 'operations',
        name: 'Operations',
        description: 'Fulfilment, inventory and support data.',
        color: '#8B5CF6',
        assets: 415,
        products: 2,
    },
];

function displayProperties(color) {
    return { __typename: 'DisplayProperties', colorHex: color, icon: null };
}

function ownedBy(urn = ACTOR) {
    return {
        __typename: 'Ownership',
        owners: [
            {
                __typename: 'Owner',
                owner: {
                    __typename: 'CorpUser',
                    urn,
                    type: 'CORP_USER',
                    username: 'boot',
                    info: {
                        active: true,
                        displayName: 'Boot User',
                        title: null,
                        email: 'boot@example.com',
                        firstName: 'Boot',
                        lastName: 'User',
                        fullName: 'Boot User',
                    },
                    properties: {
                        active: true,
                        displayName: 'Boot User',
                        title: null,
                        email: 'boot@example.com',
                        firstName: 'Boot',
                        lastName: 'User',
                        fullName: 'Boot User',
                    },
                    editableProperties: { displayName: 'Boot User', title: null, pictureLink: null, email: null },
                },
                type: 'TECHNICAL_OWNER',
                ownershipType: {
                    __typename: 'OwnershipTypeEntity',
                    urn: 'urn:li:ownershipType:__system__technical_owner',
                    type: 'CUSTOM_OWNERSHIP_TYPE',
                    info: {
                        name: 'Technical Owner',
                        description: 'Involved in the production, maintenance, or distribution of the asset(s).',
                    },
                    status: { removed: false },
                },
                associatedUrn: urn,
                attribution: null,
            },
        ],
        lastModified: { time: RECORDED_AT - 30 * DAY_MS },
    };
}

function domainEntity(domain) {
    return {
        __typename: 'Domain',
        urn: `urn:li:domain:${domain.id}`,
        type: 'DOMAIN',
        id: domain.id,
        properties: { __typename: 'DomainProperties', name: domain.name, description: domain.description },
        parentDomains: { __typename: 'ParentDomainsResult', count: 0, domains: [] },
        displayProperties: displayProperties(domain.color),
        ownership: ownedBy(),
        entities: { __typename: 'SearchResults', total: domain.assets },
        dataProducts: { __typename: 'SearchResults', total: domain.products },
        applicationsInDomain: { __typename: 'SearchResults', total: 0 },
        children: { __typename: 'EntityRelationshipsResult', total: 0 },
        deprecation: null,
    };
}

function domainAssociation(domain, associatedUrn) {
    return {
        __typename: 'DomainAssociation',
        domain: {
            __typename: 'Domain',
            urn: `urn:li:domain:${domain.id}`,
            type: 'DOMAIN',
            properties: { __typename: 'DomainProperties', name: domain.name, description: domain.description },
            parentDomains: { __typename: 'ParentDomainsResult', count: 0, domains: [] },
            displayProperties: displayProperties(domain.color),
            deprecation: null,
        },
        associatedUrn,
        attribution: null,
    };
}

function datasetUrn(platformName, name) {
    return `urn:li:dataset:(urn:li:dataPlatform:${platformName},${name},PROD)`;
}

// Owned assets: datasets, a dashboard and a chart, as searchAcrossEntities returns them.
const OWNED = [
    {
        kind: 'dataset',
        platform: 'snowflake',
        name: 'analytics.finance.fct_invoices',
        subType: 'Table',
        domain: 0,
        daysAgo: 1,
    },
    {
        kind: 'dataset',
        platform: 'snowflake',
        name: 'analytics.finance.dim_customers',
        subType: 'Table',
        domain: 0,
        daysAgo: 2,
    },
    {
        kind: 'dataset',
        platform: 'dbt',
        name: 'analytics.product.daily_active_users',
        subType: 'Model',
        domain: 2,
        daysAgo: 1,
    },
    {
        kind: 'dataset',
        platform: 'snowflake',
        name: 'analytics.marketing.campaign_touches',
        subType: 'View',
        domain: 1,
        daysAgo: 4,
    },
    { kind: 'dataset', platform: 'kafka', name: 'events.checkout_completed', subType: 'Topic', domain: 3, daysAgo: 6 },
    { kind: 'dashboard', platform: 'looker', id: 'revenue_overview', name: 'Revenue Overview', domain: 0, daysAgo: 3 },
    { kind: 'chart', platform: 'looker', id: 'weekly_signups', name: 'Weekly Signups', domain: 1, daysAgo: 5 },
];

function containerPath(platformName, name) {
    const parts = name.split('.').slice(0, -1);
    return parts.map((part, i) => ({
        __typename: 'Container',
        urn: `urn:li:container:${platformName}_${parts.slice(0, i + 1).join('_')}`,
        type: 'CONTAINER',
        platform: platform(platformName),
        properties: { __typename: 'ContainerProperties', name: part },
        subTypes: { __typename: 'SubTypes', typeNames: [i === 0 ? 'Database' : 'Schema'] },
        deprecation: null,
    }));
}

function parentContainers(platformName, name) {
    const containers = containerPath(platformName, name).reverse();
    return { __typename: 'ParentContainersResult', count: containers.length, containers };
}

function browsePath(platformName, name) {
    return {
        __typename: 'BrowsePathV2',
        path: containerPath(platformName, name).map((container) => ({
            __typename: 'BrowsePathEntry',
            name: container.properties.name,
            entity: container,
        })),
    };
}

function cardEntity(asset) {
    const lastModified = { __typename: 'AuditStamp', time: RECORDED_AT - asset.daysAgo * DAY_MS, actor: ACTOR };
    if (asset.kind === 'dataset') {
        const shortName = asset.name.split('.').pop();
        return {
            __typename: 'Dataset',
            urn: datasetUrn(asset.platform, asset.name),
            type: 'DATASET',
            exists: true,
            name: asset.name,
            platform: platform(asset.platform),
            dataPlatformInstance: null,
            properties: { __typename: 'DatasetProperties', name: shortName, qualifiedName: asset.name, lastModified },
            parentContainers: parentContainers(asset.platform, asset.name),
            subTypes: { __typename: 'SubTypes', typeNames: [asset.subType] },
            siblings: null,
            siblingsSearch: { __typename: 'ScrollResults', count: 0, total: 0, searchResults: [] },
            deprecation: null,
            browsePathV2: browsePath(asset.platform, asset.name),
        };
    }
    const typename = asset.kind === 'dashboard' ? 'Dashboard' : 'Chart';
    return {
        __typename: typename,
        urn: `urn:li:${asset.kind}:(${asset.platform},${asset.id})`,
        type: asset.kind.toUpperCase(),
        tool: asset.platform,
        [asset.kind === 'dashboard' ? 'dashboardId' : 'chartId']: asset.id,
        properties: { __typename: `${typename}Properties`, name: asset.name, lastModified },
        platform: platform(asset.platform),
        dataPlatformInstance: null,
        parentContainers: { __typename: 'ParentContainersResult', count: 0, containers: [] },
        subTypes: { __typename: 'SubTypes', typeNames: [asset.kind === 'dashboard' ? 'Dashboard' : 'Look'] },
        deprecation: null,
        browsePathV2: { __typename: 'BrowsePathV2', path: [] },
    };
}

function recommendationEntity(asset) {
    const card = cardEntity(asset);
    const common = {
        globalTags: null,
        glossaryTerms: null,
        domain: domainAssociation(DOMAINS[asset.domain], card.urn),
        deprecation: null,
        health: [],
    };
    if (asset.kind === 'dataset') {
        return {
            __typename: 'Dataset',
            urn: card.urn,
            type: card.type,
            name: asset.name,
            platform: card.platform,
            editableProperties: null,
            properties: { __typename: 'DatasetProperties', name: card.properties.name },
            subTypes: card.subTypes,
            ...common,
        };
    }
    return {
        __typename: card.__typename,
        urn: card.urn,
        type: card.type,
        tool: card.tool,
        [asset.kind === 'dashboard' ? 'dashboardId' : 'chartId']: asset.id,
        properties: { __typename: card.properties.__typename, name: asset.name },
        platform: card.platform,
        subTypes: card.subTypes,
        ...common,
    };
}

function ownerValues(variables) {
    const filters = variables?.input?.filters || variables?.input?.orFilters?.flatMap((f) => f.and || []) || [];
    return filters.find((filter) => filter.field === 'owners')?.values || [];
}

// Your Assets asks for all types; Domains/Glossary/Tags "you own" ask for one type each.
function getSearchResultsForMultipleCards(variables = {}) {
    const input = variables.input || {};
    const types = input.types || [];
    const start = input.start || 0;
    const count = input.count ?? 10;
    let entities = [];
    if (ownerValues(variables).includes(ACTOR)) {
        if (types.length === 0) entities = OWNED.map(cardEntity);
        else if (types.includes('DOMAIN')) entities = DOMAINS.slice(0, 2).map(domainEntity);
    }
    const page = entities.slice(start, start + count);
    return {
        searchAcrossEntities: {
            __typename: 'SearchResults',
            start,
            count: page.length,
            total: entities.length,
            searchResults: page.map((entity) => ({ __typename: 'SearchResult', entity })),
        },
    };
}

const MODULE_IDS = {
    DOMAINS: 'Domains',
    PLATFORMS: 'Platforms',
    TOP_TERMS: 'TopTerms',
    TOP_TAGS: 'TopTags',
    HIGH_USAGE_ENTITIES: 'HighUsageEntities',
    RECENTLY_EDITED_ENTITIES: 'RecentlyEditedEntities',
    RECENTLY_VIEWED_ENTITIES: 'RecentlyViewedEntities',
    RECENT_SEARCHES: 'RecentSearches',
};

function searchParams(types, field, value) {
    return {
        __typename: 'RecommendationParams',
        searchParams: {
            __typename: 'SearchParams',
            types,
            query: '',
            filters: [{ __typename: 'FacetFilter', field, values: [value] }],
        },
        entityProfileParams: null,
        contentParams: null,
    };
}

function recommendationModules() {
    return {
        Domains: {
            title: 'Domains',
            renderType: 'DOMAIN_SEARCH_LIST',
            content: DOMAINS.map((domain) => {
                const entity = domainEntity(domain);
                return {
                    value: entity.urn,
                    entity,
                    params: {
                        ...searchParams(null, 'domains', entity.urn),
                        contentParams: { __typename: 'ContentParams', count: domain.assets },
                    },
                };
            }),
        },
        Platforms: {
            title: 'Platforms',
            renderType: 'PLATFORM_SEARCH_LIST',
            content: [
                ['snowflake', 18240],
                ['dbt', 6120],
                ['looker', 2380],
                ['kafka', 940],
                ['airflow', 310],
            ].map(([name, count]) => ({
                value: `urn:li:dataPlatform:${name}`,
                entity: platform(name),
                params: {
                    ...searchParams(null, 'platform', `urn:li:dataPlatform:${name}`),
                    contentParams: { __typename: 'ContentParams', count },
                },
            })),
        },
        RecentlyViewedEntities: {
            title: 'Recently Viewed',
            renderType: 'ENTITY_NAME_LIST',
            content: OWNED.slice(0, 5).map((asset) => {
                const entity = recommendationEntity(asset);
                return {
                    value: entity.urn,
                    entity,
                    params: {
                        __typename: 'RecommendationParams',
                        searchParams: null,
                        entityProfileParams: { __typename: 'EntityProfileParams', urn: entity.urn, type: entity.type },
                        contentParams: null,
                    },
                };
            }),
        },
        RecentSearches: {
            title: 'Recent Searches',
            renderType: 'SEARCH_QUERY_LIST',
            content: ['invoices', 'daily active users', 'campaign'].map((query) => ({
                value: query,
                entity: null,
                params: {
                    __typename: 'RecommendationParams',
                    searchParams: { __typename: 'SearchParams', types: null, query, filters: [] },
                    entityProfileParams: null,
                    contentParams: null,
                },
            })),
        },
    };
}

// GMS ranks HOME candidates; requestContext.modules narrows them like the real resolver.
function listRecommendations(variables = {}) {
    const context = variables.input?.requestContext || {};
    const all = recommendationModules();
    let ids;
    if (Array.isArray(context.modules) && context.modules.length) {
        ids = context.modules.map((id) => MODULE_IDS[id] || id);
    } else if (context.scenario === 'SEARCH_BAR') {
        ids = ['RecentSearches'];
    } else if (context.scenario === 'HOME' || !context.scenario) {
        ids = ['RecentlyViewedEntities', 'Platforms', 'Domains'];
    } else {
        ids = [];
    }
    const limit = variables.input?.limit ?? 10;
    const modules = ids
        .filter((id) => all[id])
        .slice(0, limit)
        .map((moduleId) => ({
            __typename: 'RecommendationModule',
            title: all[moduleId].title,
            moduleId,
            renderType: all[moduleId].renderType,
            content: all[moduleId].content.map((content) => ({ __typename: 'RecommendationContent', ...content })),
        }));
    return { listRecommendations: { __typename: 'ListRecommendationsResult', modules } };
}

const LAST_VIEWED_ANNOUNCEMENT_STEP = 'lastViewedAnnouncementTimeMs';

// A step id that comes back is a step the user has seen. A returning user has
// dismissed every onboarding tour, and last opened announcements a week ago.
function batchGetStepStates(variables = {}) {
    const ids = variables.input?.ids || [];
    return {
        batchGetStepStates: {
            __typename: 'BatchGetStepStatesResult',
            results: ids.map((id) => ({
                __typename: 'StepStateResult',
                id,
                properties: id.endsWith(`-${LAST_VIEWED_ANNOUNCEMENT_STEP}`)
                    ? [
                          {
                              __typename: 'StringMapEntry',
                              key: LAST_VIEWED_ANNOUNCEMENT_STEP,
                              value: String(RECORDED_AT - 7 * DAY_MS),
                          },
                      ]
                    : [],
            })),
        },
    };
}

function getQuickFilters() {
    return {
        getQuickFilters: {
            __typename: 'GetQuickFiltersResult',
            quickFilters: ['snowflake', 'dbt', 'looker', 'kafka', 'airflow'].map((name) => ({
                __typename: 'QuickFilter',
                field: 'platform',
                value: `urn:li:dataPlatform:${name}`,
                entity: platform(name),
            })),
        },
    };
}

function getUserGroupsUrns(variables = {}) {
    const groups = variables.urn === ACTOR ? ['analytics-engineering', 'data-platform'] : [];
    return {
        corpUser: {
            __typename: 'CorpUser',
            relationships: {
                __typename: 'EntityRelationshipsResult',
                start: 0,
                count: groups.length,
                total: groups.length,
                relationships: groups.map((group) => ({
                    __typename: 'EntityRelationship',
                    entity: { __typename: 'CorpGroup', urn: `urn:li:corpGroup:${group}`, type: 'CORP_GROUP' },
                })),
            },
        },
    };
}

function view({ id, viewType, name, description, platformName }) {
    return {
        __typename: 'DataHubView',
        urn: `urn:li:dataHubView:${id}`,
        type: 'DATAHUB_VIEW',
        viewType,
        name,
        description,
        definition: {
            __typename: 'DataHubViewDefinition',
            entityTypes: [],
            filter: {
                __typename: 'DataHubViewFilter',
                operator: 'AND',
                filters: [
                    {
                        __typename: 'FacetFilter',
                        field: 'platform',
                        condition: 'EQUAL',
                        values: [`urn:li:dataPlatform:${platformName}`],
                        negated: false,
                    },
                ],
                json: null,
            },
        },
    };
}

function listViews(views, variables = {}) {
    const start = variables.start || 0;
    const page = views.slice(start, start + (variables.count ?? 20));
    return { __typename: 'ListViewsResult', start, count: page.length, total: views.length, views: page };
}

const GLOBAL_VIEWS = [
    view({
        id: 'warehouse',
        viewType: 'GLOBAL',
        name: 'Warehouse',
        description: 'Snowflake tables and views.',
        platformName: 'snowflake',
    }),
    view({ id: 'bi', viewType: 'GLOBAL', name: 'BI', description: 'Dashboards and charts.', platformName: 'looker' }),
];
const MY_VIEWS = [
    view({ id: 'my-dbt-models', viewType: 'PERSONAL', name: 'My dbt models', description: null, platformName: 'dbt' }),
];

const POSTS = [
    {
        id: 'quarterly-close',
        title: 'Finance tables freeze for quarterly close',
        description: 'Schema changes to finance.* are paused from the 28th until the books close.',
        link: null,
        daysAgo: 2,
    },
    {
        id: 'data-catalog-guide',
        title: 'New: data catalog guide',
        description: 'How we document, own and certify datasets.',
        link: 'https://example.com/data-catalog-guide',
        daysAgo: 9,
    },
];

function listPosts(variables = {}) {
    const start = variables.input?.start || 0;
    const posts = POSTS.map((post) => ({
        __typename: 'Post',
        urn: `urn:li:post:${post.id}`,
        type: 'POST',
        postType: 'HOME_PAGE_ANNOUNCEMENT',
        lastModified: { __typename: 'AuditStamp', time: RECORDED_AT - post.daysAgo * DAY_MS },
        content: {
            __typename: 'PostContent',
            contentType: post.link ? 'LINK' : 'TEXT',
            title: post.title,
            description: post.description,
            link: post.link,
            media: null,
        },
    })).slice(start, start + (variables.input?.count ?? 10));
    return {
        listPosts: { __typename: 'ListPostsResult', start, count: posts.length, total: POSTS.length, posts },
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
    batchGetStepStates,
    getQuickFilters,
    getSearchResultsForMultipleCards,
    getUserGroupsUrns,
    listGlobalViews: (variables) => ({ listGlobalViews: listViews(GLOBAL_VIEWS, variables) }),
    listMyViews: (variables) => ({ listMyViews: listViews(MY_VIEWS, variables) }),
    listPosts,
    listRecommendations,
};

export function hasGraphqlFixture(operationName) {
    return typeof operationName === 'string' && Object.hasOwn(FIXTURES, operationName);
}

export function graphqlStubData(operationName, variables = {}) {
    if (!hasGraphqlFixture(operationName)) return {};
    return FIXTURES[operationName](variables || {});
}
