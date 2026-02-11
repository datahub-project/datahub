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

export const showSeparateSiblingsRef = { current: { showSeparateSiblings: false } };

export function useIsShowSeparateSiblingsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showSeparateSiblings;
}

export function useShowIntroducePage() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showIntroducePage;
}

/**
 * Hook to check if Context Documents feature is enabled.
 */
export function useIsContextDocumentsEnabled(): boolean {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.contextDocumentsEnabled;
}

export const hideLineageInSearchCardsRef = { current: { hideLineageInSearchCards: false } };

export function useHideLineageInSearchCards() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.hideLineageInSearchCards;
}
