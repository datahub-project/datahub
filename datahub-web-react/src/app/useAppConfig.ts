import { useContext } from 'react';

import { HIDE_LINEAGE_IN_SEARCH_CARDS_KEY, SHOW_SEPARATE_SIBLINGS_KEY } from '@app/appConfig/UpdateGlobalFlags';
import { loadFromLocalStorage, setInLocalStorage } from '@app/sharedV2/hooks/useFeatureFlag';
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

export const showSeparateSiblingsRef = { current: { showSeparateSiblings: false } };

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

export function useIsAskDataHubModeSelectEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAskDataHubModeSelect;
}

export function useIsAiChatFeedbackEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAskDataHubFeedback;
}

export function useIsFreshnessAssertionTuningEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.freshnessAssertionTuningEnabled;
}

export function useIsContextDocumentsEnabled(): boolean {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.contextDocumentsEnabled;
}

export const hideLineageInSearchCardsRef = { current: { hideLineageInSearchCards: false } };

const FREE_TRIAL_INSTANCE_KEY = 'isFreeTrialInstance';

export function useIsFreeTrialInstance() {
    const appConfig = useAppConfig();
    const isFreeTrial = appConfig.config.trialConfig.trialEnabled;

    if (appConfig.loaded) {
        setInLocalStorage(FREE_TRIAL_INSTANCE_KEY, isFreeTrial);
        return isFreeTrial;
    }

    return loadFromLocalStorage(FREE_TRIAL_INSTANCE_KEY);
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
