import { useContext } from 'react';

import { loadFromLocalStorage, setInLocalStorage } from '@app/sharedV2/hooks/useFeatureFlag';
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

export function useIsAiChatFeedbackEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAskDataHubFeedback;
}

export function useIsFreshnessAssertionTuningEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.freshnessAssertionTuningEnabled;
}

const FREE_TRIAL_INSTANCE_KEY = 'isFreeTrialInstance';

/**
 * Check if the instance is configured for a free trial
 */
export function useIsFreeTrialInstance() {
    const appConfig = useAppConfig();
    const isFreeTrial = appConfig.config.trialConfig.trialEnabled;

    if (appConfig.loaded) {
        setInLocalStorage(FREE_TRIAL_INSTANCE_KEY, isFreeTrial);
        return isFreeTrial;
    }

    return loadFromLocalStorage(FREE_TRIAL_INSTANCE_KEY);
}

/**
 * Hook to check if Context Documents feature is enabled.
 */
export function useIsContextDocumentsEnabled(): boolean {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.contextDocumentsEnabled;
}
