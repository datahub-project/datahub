import { useEffect } from 'react';
import { useAppConfig } from './useAppConfig';
import { useIsThemeV2 } from './useIsThemeV2';

export function useShowNavBarRedesign() {
    const appConfig = useAppConfig();
    const isThemeV2Enabled = useIsThemeV2();

    if (!appConfig.loaded) {
        return loadFromLocalStorage();
    }

    return appConfig.config.featureFlags.showNavBarRedesign && isThemeV2Enabled;
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
