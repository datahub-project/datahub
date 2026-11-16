// TODO: Move useAppConfig and AppConfigProvider into this directory
import { useEffect } from 'react';

import { loadFromLocalStorage, setInLocalStorage } from '@app/sharedV2/hooks/useFeatureFlag';
import { useAppConfig } from '@app/useAppConfig';
import { AppConfigWithoutPolicyPrivileges } from '@src/appConfigContext';

export const SHOW_SEPARATE_SIBLINGS_KEY = 'showSeparateSiblings';
export const HIDE_LINEAGE_IN_SEARCH_CARDS_KEY = 'hideLineageInSearchCards';

function loadPersistedFlag(key: string): boolean {
    try {
        return loadFromLocalStorage(key);
    } catch {
        // Storage can be unavailable at module load (disabled storage, sandboxed iframe);
        // a throw here would take down the whole bundle.
        return false;
    }
}

// The Apollo link in App.tsx reads these refs on every request, including the ones fired
// before the appConfig query resolves. Seed them from the values the previous page load
// persisted so the first search of a session already sends the configured flags.
export const hideLineageInSearchCardsRef = { current: loadPersistedFlag(HIDE_LINEAGE_IN_SEARCH_CARDS_KEY) };
export const showSeparateSiblingsRef = { current: loadPersistedFlag(SHOW_SEPARATE_SIBLINGS_KEY) };

export default function UpdateGlobalFlags() {
    useUpdateGlobalFlag(
        showSeparateSiblingsRef,
        SHOW_SEPARATE_SIBLINGS_KEY,
        (appConfig) => appConfig.featureFlags.showSeparateSiblings,
    );

    useUpdateGlobalFlag(
        hideLineageInSearchCardsRef,
        HIDE_LINEAGE_IN_SEARCH_CARDS_KEY,
        (appConfig) => appConfig.featureFlags.hideLineageInSearchCards,
    );

    return null;
}

function useUpdateGlobalFlag(
    ref: { current: boolean },
    localStorageKey: string,
    getValue: (appConfig: AppConfigWithoutPolicyPrivileges) => boolean,
) {
    const { config, loaded } = useAppConfig();
    const value = getValue(config);

    useEffect(() => {
        if (loaded) {
            ref.current = value; // eslint-disable-line no-param-reassign
            setInLocalStorage(localStorageKey, value);
        }
    }, [ref, localStorageKey, loaded, value]);
}
