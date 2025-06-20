import React from 'react';

import { AppConfig, PersonalSidebarSection, SearchBarApi } from '@types';

export const DEFAULT_APP_CONFIG = {
    analyticsConfig: {
        enabled: false,
    },
    policiesConfig: {
        enabled: false,
        platformPrivileges: [],
        resourcePrivileges: [],
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
        entityProfile: {
            domainDefaultTab: null,
        },
        searchResult: {
            enableNameHighlight: false,
        },
    },
    authConfig: {
        tokenAuthEnabled: false,
    },
    telemetryConfig: {
        enableThirdPartyLogging: false,
    },
    testsConfig: {
        enabled: false,
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
        dataContractsEnabled: false,
        editableDatasetNameEnabled: false,
        themeV2Enabled: false,
        themeV2Default: false,
        themeV2Toggleable: false,
        lineageGraphV2: false,
        showSeparateSiblings: false,
        schemaFieldCLLEnabled: false,
        schemaFieldLineageIgnoreStatus: false,
        showManageStructuredProperties: false,
        hideDbtSourceInLineage: false,
        showNavBarRedesign: false,
        showAutoCompleteResults: false,
        entityVersioningEnabled: false,
        showHasSiblingsFilter: false,
        showSearchBarAutocompleteRedesign: false,
        showManageTags: false,
        showIntroducePage: false,
        showIngestionPageRedesign: false,
        showLineageExpandMore: false,
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
