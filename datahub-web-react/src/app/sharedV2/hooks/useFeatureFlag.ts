import { useAppConfig } from '@app/useAppConfig';

export function useFeatureFlag(featureFlagKey: string) {
    const appConfig = useAppConfig();

    const featureFlagValue = appConfig?.config?.featureFlags?.[featureFlagKey];

    if (appConfig.loaded) {
        setFeatureFlagInLocalStorage(featureFlagKey, featureFlagValue);
        return featureFlagValue;
    }

    return loadFeatureFlagFromLocalStorage(featureFlagKey);
}

function setFeatureFlagInLocalStorage(flagKey: string, value: boolean) {
    const rawValue = localStorage.getItem(flagKey);
    const storedValue = rawValue === null ? undefined : rawValue === 'true';

    if (rawValue === null || storedValue !== value) {
        saveFeatureFlagToLocalStorage(flagKey, value);
    }
}

export function loadFeatureFlagFromLocalStorage(flagKey: string): boolean {
    return localStorage.getItem(flagKey) === 'true';
}

function saveFeatureFlagToLocalStorage(flagKey: string, value: boolean) {
    localStorage.setItem(flagKey, String(value));
}
