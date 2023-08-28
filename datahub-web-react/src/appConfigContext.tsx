import React from 'react';
import { AppConfig } from './types.generated';

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
    featureFlags: {
        readOnlyModeEnabled: false,
        showSearchFiltersV2: true,
        showBrowseV2: true,
        showAcrylInfo: false,
    },
};

export const AppConfigContext = React.createContext<{
    config: AppConfig;
    refreshContext: () => void;
}>({ config: DEFAULT_APP_CONFIG, refreshContext: () => null });
