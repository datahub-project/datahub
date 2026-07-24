// TODO: Move useAppConfig and AppConfigProvider into this directory
import { useEffect } from 'react';

import { setInLocalStorage } from '@app/sharedV2/hooks/useFeatureFlag';
import { useAppConfig } from '@app/useAppConfig';

import { AppConfig } from '@types';

declare global {
    interface Window {
        __INITIAL_FLAGS__?: {
            showSeparateSiblings?: boolean;
            hideLineageInSearchCards?: boolean;
        };
    }
}

export const showSeparateSiblingsRef = { current: window.__INITIAL_FLAGS__?.showSeparateSiblings ?? false };
export const hideLineageInSearchCardsRef = { current: window.__INITIAL_FLAGS__?.hideLineageInSearchCards ?? false };

export const SHOW_SEPARATE_SIBLINGS_KEY = 'showSeparateSiblings';
export const HIDE_LINEAGE_IN_SEARCH_CARDS_KEY = 'hideLineageInSearchCards';

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
    getValue: (appConfig: AppConfig) => boolean,
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
