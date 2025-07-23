import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';

export function useShowHomePageRedesign() {
    const appConfig = useAppConfig();
    const { showHomePageRedesign } = appConfig.config.featureFlags;

    if (appConfig.loaded) {
        setShowHomePageRedesignLocalStorage(showHomePageRedesign);

        return showHomePageRedesign;
    }

    // Default before flags loaded is stored in local storage
    return loadHomePageRedesignFromLocalStorage();
}

function setShowHomePageRedesignLocalStorage(showHomePageRedesign: boolean) {
    if (loadHomePageRedesignFromLocalStorage() !== showHomePageRedesign) {
        saveToLocalStorage(showHomePageRedesign);
    }
}

const HOME_PAGE_REDESIGN_KEY = 'showHomePageRedesign';

export function loadHomePageRedesignFromLocalStorage(): boolean {
    return localStorage.getItem(HOME_PAGE_REDESIGN_KEY) === 'true';
}

function saveToLocalStorage(isThemeV2: boolean) {
    localStorage.setItem(HOME_PAGE_REDESIGN_KEY, String(isThemeV2));
}
