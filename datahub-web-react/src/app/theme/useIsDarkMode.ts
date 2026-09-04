import { useCallback, useEffect, useRef, useState } from 'react';

import { loadFromLocalStorage, useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

const DARK_MODE_KEY = 'isDarkModeEnabled';
const DARK_MODE_CHANGE_EVENT = 'datahub-darkmode-change';

export const THEME_DARK_MODE_FLAG = 'themeDarkModeEnabled';

function loadDarkModeFromLocalStorage(): boolean {
    const item = localStorage.getItem(DARK_MODE_KEY);
    if (item === null) return false;
    return item === 'true';
}

function saveDarkModeToLocalStorage(isDark: boolean) {
    localStorage.setItem(DARK_MODE_KEY, String(isDark));
}

/**
 * Hook that provides the current dark mode state and a toggle function.
 * Persisted in localStorage; defaults to light mode.
 * Dark mode only applies when the themeDarkModeEnabled feature flag is on.
 *
 * All hook instances in the same tab stay in sync via a custom window event.
 */
export function useIsDarkMode(): [boolean, () => void] {
    const darkModeEnabled = useFeatureFlag(THEME_DARK_MODE_FLAG);
    const [isDarkMode, setIsDarkMode] = useState(loadDarkModeFromLocalStorage);
    const isFirstRender = useRef(true);

    const toggleDarkMode = useCallback(() => {
        if (!darkModeEnabled) {
            return;
        }
        setIsDarkMode((prev) => !prev);
    }, [darkModeEnabled]);

    // Persist to localStorage and notify other instances when state changes (skip first render)
    useEffect(() => {
        if (isFirstRender.current) {
            isFirstRender.current = false;
            return;
        }
        saveDarkModeToLocalStorage(isDarkMode);
        window.dispatchEvent(new Event(DARK_MODE_CHANGE_EVENT));
    }, [isDarkMode]);

    // Sync with other hook instances when they toggle dark mode
    useEffect(() => {
        const syncHandler = () => {
            setIsDarkMode(loadDarkModeFromLocalStorage());
        };
        window.addEventListener(DARK_MODE_CHANGE_EVENT, syncHandler);
        return () => window.removeEventListener(DARK_MODE_CHANGE_EVENT, syncHandler);
    }, []);

    return [Boolean(darkModeEnabled) && isDarkMode, toggleDarkMode];
}

/**
 * Reads the dark mode preference from localStorage without React state.
 * Used for initial theme selection before hooks are available.
 * Requires the cached feature flag to be on so a previous preference cannot
 * enable dark mode after the flag is turned off.
 */
export function loadIsDarkMode(): boolean {
    return loadFromLocalStorage(THEME_DARK_MODE_FLAG) && loadDarkModeFromLocalStorage();
}
