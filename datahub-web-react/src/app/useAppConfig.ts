import { useContext } from 'react';

import { HIDE_LINEAGE_IN_SEARCH_CARDS_KEY, SHOW_SEPARATE_SIBLINGS_KEY } from '@app/appConfig/UpdateGlobalFlags';
import { loadFromLocalStorage } from '@app/sharedV2/hooks/useFeatureFlag';
import { AppConfigContext } from '@src/appConfigContext';

import { AppConfig } from '@types';

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

function useFlagWithLocalStorageSync(key: string, f: (appConfig: AppConfig) => boolean) {
    const { config, loaded } = useAppConfig();
    const flagValue = f(config);

    if (loaded) return flagValue;
    return loadFromLocalStorage(key);
}

export function useHideLineageInSearchCards() {
    return useFlagWithLocalStorageSync(
        HIDE_LINEAGE_IN_SEARCH_CARDS_KEY,
        (config) => config.featureFlags.hideLineageInSearchCards,
    );
}

export function useIsShowSeparateSiblingsEnabled() {
    return useFlagWithLocalStorageSync(
        SHOW_SEPARATE_SIBLINGS_KEY,
        (config) => config.featureFlags.showSeparateSiblings,
    );
}
