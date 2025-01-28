import { useEffect } from 'react';
import { useCustomTheme } from '../customThemeContext';
import { useAppConfig } from './useAppConfig';
import { useUserContext } from './context/useUserContext';

/**
 * Returns true if theme v2 should be enabled.
 *
 * There are 3 conditions in which theme v2 is enabled:
 * 1. If theme v2 is enabled and toggleable, and the user has enabled theme v2.
 * 2. If theme v2 is enabled and toggleable, the user has not set a preference, and v2 is the default theme.
 * 3. If theme v2 is enabled not toggleable, and v2 is the default theme.
 */
export function useIsThemeV2() {
    const appConfig = useAppConfig();
    const { themeV2Enabled, themeV2Default, themeV2Toggleable } = appConfig.config.featureFlags;
    const [isThemeV2EnabledForUser, isUserLoaded] = useIsThemeV2EnabledForUser();

    if (appConfig.loaded && !themeV2Enabled) return false;
    if (appConfig.loaded && !themeV2Toggleable) return themeV2Default;
    if (appConfig.loaded && isUserLoaded) return isThemeV2EnabledForUser;

    // Default before flags loaded is stored in local storage
    return loadThemeV2FromLocalStorage();
}

/**
 * Returns [isThemeV2Toggleable, isAppConfigLoaded]: whether the V2 theme can be toggled by users.
 */
export function useIsThemeV2Toggleable() {
    const appConfig = useAppConfig();
    return [
        appConfig.config.featureFlags.themeV2Enabled && appConfig.config.featureFlags.themeV2Toggleable,
        appConfig.loaded,
    ];
}

/**
 * Returns [isThemeV2EnabledForUser, isUserLoaded]: whether the V2 theme is turned on for the user.
 */
export function useIsThemeV2EnabledForUser(): [boolean, boolean] {
    const appConfig = useAppConfig();
    const { user } = useUserContext();
    return [
        user?.settings?.appearance?.showThemeV2 ?? appConfig.config.featureFlags.themeV2Default,
        !!user && appConfig.loaded,
    ];
}

export function useSetThemeIsV2() {
    const isThemeV2 = useIsThemeV2();
    const { updateTheme } = useCustomTheme();

    useEffect(() => {
        if (isThemeV2) {
            import('../conf/theme/theme_v2.config.json').then((theme) => updateTheme(theme));
        } else {
            import('../conf/theme/theme_light.config.json').then((theme) => updateTheme(theme));
        }
        setThemeV2LocalStorage(isThemeV2);
    }, [isThemeV2, updateTheme]);
}

function setThemeV2LocalStorage(isThemeV2: boolean) {
    if (loadThemeV2FromLocalStorage() !== isThemeV2) {
        saveToLocalStorage(isThemeV2);
    }
}

const THEME_V2_STATUS_KEY = 'isThemeV2Enabled';

export function loadThemeV2FromLocalStorage(): boolean {
    return localStorage.getItem(THEME_V2_STATUS_KEY) === 'true';
}

function saveToLocalStorage(isThemeV2: boolean) {
    localStorage.setItem(THEME_V2_STATUS_KEY, String(isThemeV2));
}
