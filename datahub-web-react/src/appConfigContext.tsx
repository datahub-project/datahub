/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    searchCardConfig: {
        showDescription: false,
    },
    searchFlagsConfig: {
        defaultSkipHighlighting: false,
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
        contextDocumentsEnabled: false,
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
        showDefaultExternalLinks: true,
        showStatsTabRedesign: false,
        showHomePageRedesign: false,
        showProductUpdates: false,
        lineageGraphV3: false,
        logicalModelsEnabled: false,
        showHomepageUserRole: false,
        assetSummaryPageV1: false,
        datasetSummaryPageV1: false,
        documentationFileUploadV1: false,
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
