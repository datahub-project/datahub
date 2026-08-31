import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';

export function useShowNavBarRedesign() {
    const appConfig = useAppConfig();

    if (!appConfig.loaded) {
        return loadFromLocalStorage();
    }

    return appConfig.config.featureFlags.showNavBarRedesign;
}

export function useSetNavBarRedesignEnabled() {
    const isNavBarRedesignEnabled = useShowNavBarRedesign();

    useEffect(() => {
        setShowNavBarRedesignLocalStorage(isNavBarRedesignEnabled);
    }, [isNavBarRedesignEnabled]);
}

function setShowNavBarRedesignLocalStorage(isNavBarRedesignEnabled: boolean) {
    if (loadFromLocalStorage() !== isNavBarRedesignEnabled) {
        saveToLocalStorage(isNavBarRedesignEnabled);
    }
}

const NAV_BAR_REDESIGN_STATUS_KEY = 'isNavBarRedesignEnabled';

function loadFromLocalStorage(): boolean {
    const localStorageItem = localStorage.getItem(NAV_BAR_REDESIGN_STATUS_KEY);
    if (localStorageItem === null) {
        return true;
    }
    return localStorageItem === 'true';
}

function saveToLocalStorage(isNavBarRedesignEnabled: boolean) {
    localStorage.setItem(NAV_BAR_REDESIGN_STATUS_KEY, String(isNavBarRedesignEnabled));
}
