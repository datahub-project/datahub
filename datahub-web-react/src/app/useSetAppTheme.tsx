import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import themes from '@conf/theme/themes';
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
    const { updateTheme } = useCustomTheme();
    const customThemeId = useCustomThemeId();

    useEffect(() => {
        setThemeIdLocalStorage(customThemeId);
    }, [customThemeId]);

    useEffect(() => {
        if (customThemeId && customThemeId.endsWith('.json')) {
            if (import.meta.env.DEV) {
                import(/* @vite-ignore */ `../../conf/theme/${customThemeId}`)
                    .then((theme) => {
                        updateTheme(theme);
                    })
                    .catch((error) => {
                        console.error(`Failed to load theme from '../../conf/theme/${customThemeId}':`, error);
                    });
            } else {
                fetch(`assets/conf/theme/${customThemeId}`)
                    .then((response) => response.json())
                    .then((theme) => {
                        updateTheme(theme);
                    })
                    .catch((error) => {
                        console.error(`Failed to load theme from 'assets/conf/theme/${customThemeId}':`, error);
                    });
            }
        } else if (customThemeId && themes[customThemeId]) {
            updateTheme(themes[customThemeId]);
        } else {
            updateTheme(isThemeV2 ? themes.themeV2 : themes.themeV1);
        }
    }, [customThemeId, isThemeV2, updateTheme]);
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
