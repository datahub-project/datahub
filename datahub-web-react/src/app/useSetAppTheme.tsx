import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { useCustomTheme } from '@src/customThemeContext';

// add new theme ids here
// eslint-disable-next-line @typescript-eslint/no-unused-vars
enum ThemeId {}

function useCustomThemeId() {
    const { config, loaded } = useAppConfig();

    if (!loaded) {
        return loadThemeIdFromLocalStorage();
    }

    return config.visualConfig.theme?.themeId || null;
}

export function useSetAppTheme() {
    const isThemeV2 = useIsThemeV2();
    const { config } = useAppConfig();
    const { updateTheme } = useCustomTheme();
    const customThemeId = useCustomThemeId();

    useEffect(() => {
        setThemeIdLocalStorage(customThemeId);
    }, [customThemeId]);

    useEffect(() => {
        // here is where we can start adding new custom themes based on customThemeId

        if (isThemeV2) {
            fetch('assets/conf/theme/theme_v2.config.json')
                .then((response) => response.json())
                .then((theme) => updateTheme(theme))
                .catch((error) => {
                    console.error('Failed to load theme_v2 config:', error);
                    // Fallback to static import as backup
                    import('../conf/theme/theme_v2.config.json').then((theme) => updateTheme(theme));
                });
        } else {
            fetch('assets/conf/theme/theme_light.config.json')
                .then((response) => response.json())
                .then((theme) => updateTheme(theme))
                .catch((error) => {
                    console.error('Failed to load theme_light config:', error);
                    // Fallback to static import as backup
                    import('../conf/theme/theme_light.config.json').then((theme) => updateTheme(theme));
                });
        }
    }, [config, isThemeV2, updateTheme, customThemeId]);
}

function setThemeIdLocalStorage(customThemeId: string | null) {
    if (!customThemeId) {
        removeThemeIdFromLocalStorage();
    } else if (loadThemeIdFromLocalStorage() !== customThemeId) {
        saveToLocalStorage(customThemeId);
    }
}

const CUSTOM_THEME_ID_KEY = 'customThemeId';

export function loadThemeIdFromLocalStorage(): string | null {
    return localStorage.getItem(CUSTOM_THEME_ID_KEY);
}

function removeThemeIdFromLocalStorage() {
    return localStorage.removeItem(CUSTOM_THEME_ID_KEY);
}

function saveToLocalStorage(customThemeId: string) {
    localStorage.setItem(CUSTOM_THEME_ID_KEY, customThemeId);
}
