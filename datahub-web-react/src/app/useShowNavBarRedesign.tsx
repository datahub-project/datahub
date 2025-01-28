import { useEffect } from 'react';
import { useAppConfig } from './useAppConfig';

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
        setThemeV2LocalStorage(isNavBarRedesignEnabled);
    }, [isNavBarRedesignEnabled]);
}

function setThemeV2LocalStorage(isNavBarRedesignEnabled: boolean) {
    if (loadFromLocalStorage() !== isNavBarRedesignEnabled) {
        saveToLocalStorage(isNavBarRedesignEnabled);
    }
}

const NAV_BAR_REDESIGN_STATUS_KEY = 'isNavBarRedesignEnabled';

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(NAV_BAR_REDESIGN_STATUS_KEY) === 'true';
}

function saveToLocalStorage(isNavBarRedesignEnabled: boolean) {
    localStorage.setItem(NAV_BAR_REDESIGN_STATUS_KEY, String(isNavBarRedesignEnabled));
}
