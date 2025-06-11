import React from 'react';

import { AppConfig, PersonalSidebarSection, SearchBarApi } from '@types';

export const DEFAULT_APP_CONFIG: AppConfig = {
    analyticsConfig: {
        enabled: false,
    },
    policiesConfig: {
        enabled: false,
        platformPrivileges: [],
        resourcePrivileges: [],
    },
    actionRequestsConfig: {
        enabled: false,
    },
    identityManagementConfig: {
        enabled: false,
    },
    managedIngestionConfig: {
        enabled: false,
    },
    lineageConfig: {
        supportsImpactAnalysis: false,
    },
    visualConfig: {
        logoUrl: undefined,
        queriesTab: {
            queriesTabResultSize: 5,
        },
        entityProfiles: {
            domain: {
                defaultTab: null,
            },
        },
        searchResult: {
            enableNameHighlight: false,
        },
        hideGlossary: false,
    },
    authConfig: {
        tokenAuthEnabled: false,
    },
    telemetryConfig: {
        enableThirdPartyLogging: false,
        userTrackingEnabled: false,
    },
    testsConfig: {
        enabled: false,
        executionLimitConfig: {
            elasticSearchExecutor: 10000,
            defaultExecutor: 1000,
        },
    },
    viewsConfig: {
        enabled: false,
    },
    searchBarConfig: {
        apiVariant: SearchBarApi.AutocompleteForMultiple,
    },
    homePageConfig: {
        firstInPersonalSidebar: PersonalSidebarSection.YourAssets,
    },
    classificationConfig: {
        enabled: false,
        automations: {
            snowflake: false,
            aiTermClassification: false,
        },
    },
    featureFlags: {
        readOnlyModeEnabled: false,
        showSearchFiltersV2: true,
        showBrowseV2: true,
        platformBrowseV2: false,
        assertionMonitorsEnabled: false,
        schemaAssertionMonitorsEnabled: false,
        runAssertionsEnabled: false,
        subscriptionsEnabled: false,
        datasetHealthDashboardEnabled: false,
        businessAttributeEntityEnabled: false,
        showAcrylInfo: false,
        erModelRelationshipFeatureEnabled: false,
        showAccessManagement: false,
        nestedDomainsEnabled: true,
        dataContractsEnabled: false,
        aiFeaturesEnabled: false,
        themeV2Enabled: false,
        themeV2Default: false,
        themeV2Toggleable: false,
        lineageGraphV2: false,
        metadataShareEnabled: false,
        documentationFormsEnabled: false,
        emailNotificationsEnabled: false,
        slackBotTokensConfigEnabled: false,
        slackBotTokensObfuscationEnabled: false,
        showSeparateSiblings: false,
        formCreationEnabled: false,
        schemaFieldCLLEnabled: false,
        editableDatasetNameEnabled: false,
        hideDbtSourceInLineage: false,
        showBulkFormByDefault: false,
        schemaFieldLineageIgnoreStatus: false,
        showDatasetFeaturesSearchSortOptions: false,
        showManageStructuredProperties: false,
        showNavBarRedesign: false,
        showAutoCompleteResults: false,
        showFormAnalytics: false,
        showStatsTabRedesign: false,
        requestMinimalSlackPermissions: false,
        entityVersioningEnabled: false,
        showHasSiblingsFilter: false,
        showSearchBarAutocompleteRedesign: false,
        showManageTags: false,
        showIntroducePage: false,
        showLineageExpandMore: false,
        showTaskCenterRedesign: false,
        usePropagationsFramework: false,
        displayExecutorPools: false,
        onlineSmartAssertionsEnabled: false,
        showDefaultExternalLinks: true,
        showCreatedAtFilter: false,
        showIngestionPageRedesign: false,
        showProductUpdates: false,
        formsNotificationsEnabled: false,
        lineageGraphV3: false,
    },
    chromeExtensionConfig: {
        enabled: false,
        lineageEnabled: false,
    },
};

export const AppConfigContext = React.createContext<{
    config: AppConfig;
    loaded: boolean;
    refreshContext: () => void;
}>({ config: DEFAULT_APP_CONFIG, loaded: false, refreshContext: () => null });
