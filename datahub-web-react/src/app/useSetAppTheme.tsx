import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';
import { useIsDarkMode } from '@app/useIsDarkMode';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import light from '@conf/theme/colorThemes/light';
import themes from '@conf/theme/themes';
import { Theme } from '@conf/theme/types';
import { useCustomTheme } from '@src/customThemeContext';

export function useCustomThemeId(): string | null {
    const { config, loaded } = useAppConfig();

    if (import.meta.env.REACT_APP_THEME) {
        return import.meta.env.REACT_APP_THEME;
    }

    if (!loaded) {
        return loadThemeIdFromLocalStorage();
    }

    return config.visualConfig.theme?.themeId || null;
}

export function useSetAppTheme() {
    const isThemeV2 = useIsThemeV2();
    const [isDarkMode] = useIsDarkMode();
    const { updateTheme } = useCustomTheme();
    const customThemeId = useCustomThemeId();

    useEffect(() => {
        setThemeIdLocalStorage(customThemeId);
    }, [customThemeId]);

    useEffect(() => {
        if (customThemeId && customThemeId.endsWith('.json')) {
            if (import.meta.env.DEV) {
                import(/* @vite-ignore */ `./conf/theme/${customThemeId}`)
                    .then((theme) => {
                        updateTheme(ensureThemeColors(theme));
                    })
                    .catch((error) => {
                        console.error(`Failed to load theme from './conf/theme/${customThemeId}':`, error);
                    });
            } else {
                fetch(`assets/conf/theme/${customThemeId}`)
                    .then((response) => response.json())
                    .then((theme) => {
                        updateTheme(ensureThemeColors(theme));
                    })
                    .catch((error) => {
                        console.error(`Failed to load theme from 'assets/conf/theme/${customThemeId}':`, error);
                    });
            }
        } else if (customThemeId && themes[customThemeId]) {
            updateTheme(themes[customThemeId]);
        } else if (isThemeV2 && isDarkMode) {
            updateTheme(themes.themeV2Dark);
        } else {
            updateTheme(isThemeV2 ? themes.themeV2 : themes.themeV1);
        }
    }, [customThemeId, isDarkMode, isThemeV2, updateTheme]);
}

function setThemeIdLocalStorage(customThemeId: string | null) {
    if (!customThemeId) {
        removeThemeIdFromLocalStorage();
    } else if (loadThemeIdFromLocalStorage() !== customThemeId) {
        saveToLocalStorage(customThemeId);
    }
}

const CUSTOM_THEME_ID_KEY = 'customThemeId';

function loadThemeIdFromLocalStorage(): string | null {
    return localStorage.getItem(CUSTOM_THEME_ID_KEY);
}

function removeThemeIdFromLocalStorage() {
    return localStorage.removeItem(CUSTOM_THEME_ID_KEY);
}

function saveToLocalStorage(customThemeId: string) {
    localStorage.setItem(CUSTOM_THEME_ID_KEY, customThemeId);
}

/**
 * Ensures a theme loaded from JSON always has the `colors` property.
 * Customer-provided JSON themes may not include semantic color tokens,
 * so we fall back to the light theme colors to prevent runtime errors.
 */
function ensureThemeColors(theme: Partial<Theme> & Omit<Theme, 'colors'>): Theme {
    return {
        ...theme,
        colors: theme.colors ?? light,
    } as Theme;
}
