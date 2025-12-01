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

export function useIsShowCreatedAtFilter() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showCreatedAtFilter;
}

export function useIsDocumentationFormsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.documentationFormsEnabled;
}

export function useIsDatasetFeaturesSearchSortEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showDatasetFeaturesSearchSortOptions;
}

export function useIsInviteUsersEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.inviteUsersEnabled;
}

export function useIsAiChatEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAskDataHub;
}

/**
 * Check if the instance is configured for a free trial
 */
export function useIsFreeTrialInstance() {
    const appConfig = useAppConfig();
    return appConfig.config.trialConfig.trialEnabled;
}
