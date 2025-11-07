import { useContext } from 'react';

import { AppConfigContext } from '@src/appConfigContext';

/**
 * Fetch an instance of AppConfig from the React context.
 */
export function useAppConfig() {
    return useContext(AppConfigContext);
}

export function useIsShowAcrylInfoEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAcrylInfo;
}

export function useIsNestedDomainsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.nestedDomainsEnabled;
}

export function useBusinessAttributesFlag() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.businessAttributeEntityEnabled;
}

export function useIsAppConfigContextLoaded() {
    const appConfig = useAppConfig();
    return appConfig.loaded;
}

export function useIsEditableDatasetNameEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.editableDatasetNameEnabled;
}

export function useIsShowSeparateSiblingsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showSeparateSiblings;
}

export function useShowIntroducePage() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showIntroducePage;
}

/**
 * Hook to check if Context Base (documents) feature is enabled.
 * TODO: Replace with actual feature flag once backend support is added.
 */
export function useIsContextBaseEnabled(): boolean {
    // Mock feature flag - always return true for now
    // In the future, this will check the app config or a feature flag service
    // Example: return appConfig.config.featureFlags.contextBaseEnabled ?? false;
    return true;
}
