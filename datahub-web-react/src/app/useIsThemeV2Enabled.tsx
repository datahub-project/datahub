import { useEffect } from 'react';
import { useCustomTheme } from '../customThemeContext';
import { useAppConfig } from './useAppConfig';
import { useUserContext } from './context/useUserContext';

/**
 * Returns true if theme v2 should be enabled. There are 2 conditions in which theme v2 is enabled:
 *
 * 1. If the user has enabled theme v2 in their settings, theme v2 will be enabled for the user.
 * 2. If theme v2 is enabled globally, theme v2 will be enabled for the user.
 *
 * If theme v2 is enabled globally, it will take precedence over the user's settings.
 */
export function useIsThemeV2Enabled() {
    const [isThemeV2EnabledGlobally, isGlobalLoaded] = useIsThemeV2EnabledGlobally();
    const [isThemeV2EnabledForUser, isUserLoaded] = useIsThemeV2EnabledForUser();

    if (isGlobalLoaded && isThemeV2EnabledGlobally) {
        return true;
    }
    if (isUserLoaded) {
        return isThemeV2EnabledForUser;
    }

    // Default before flags loaded is stored in local storage
    return loadFromLocalStorage();
}

/**
 * Returns [isThemeV2EnabledForUser, isUserLoaded]
 */
export function useIsThemeV2EnabledForUser(): [boolean, boolean] {
    const { user } = useUserContext();
    return [!!user?.settings?.appearance?.showThemeV2, !!user];
}

/**
 * Returns [isThemeV2EnabledGlobally, isAppConfigLoaded]
 */
export function useIsThemeV2EnabledGlobally(): [boolean, boolean] {
    const appConfig = useAppConfig();
    return [appConfig.config.featureFlags.themeV2, appConfig.loaded];
}

export function useSetThemeIsV2() {
    const isThemeV2 = useIsThemeV2Enabled();
    const { updateTheme } = useCustomTheme();

    useEffect(() => {
        if (isThemeV2) {
            import('../conf/theme/theme_acryl_v2.config.json').then((theme) => updateTheme(theme));
        } else {
            import('../conf/theme/theme_acryl.config.json').then((theme) => updateTheme(theme));
        }
        setThemeV2LocalStorage(isThemeV2);
    }, [isThemeV2, updateTheme]);
}

function setThemeV2LocalStorage(isThemeV2: boolean) {
    if (loadFromLocalStorage()) {
        saveToLocalStorage(isThemeV2);
    }
}

const THEME_V2_STATUS_KEY = 'isThemeV2Enabled';

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(THEME_V2_STATUS_KEY) === 'true';
}

function saveToLocalStorage(isThemeV2: boolean) {
    localStorage.setItem(THEME_V2_STATUS_KEY, String(isThemeV2));
}
