import { useCallback, useEffect, useState } from 'react';

const DARK_MODE_KEY = 'isDarkModeEnabled';
const DARK_MODE_CHANGE_EVENT = 'datahub-darkmode-change';

function getSystemPrefersDark(): boolean {
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

function loadDarkModeFromLocalStorage(): boolean {
    const item = localStorage.getItem(DARK_MODE_KEY);
    if (item === null) return getSystemPrefersDark();
    return item === 'true';
}

function saveDarkModeToLocalStorage(isDark: boolean) {
    localStorage.setItem(DARK_MODE_KEY, String(isDark));
}

/**
 * Hook that provides the current dark mode state and a toggle function.
 * Persisted in localStorage; defaults to the OS-level preference.
 *
 * All hook instances in the same tab stay in sync via a custom window event.
 */
export function useIsDarkMode(): [boolean, () => void] {
    const [isDarkMode, setIsDarkMode] = useState(loadDarkModeFromLocalStorage);

    const toggleDarkMode = useCallback(() => {
        setIsDarkMode((prev) => {
            const next = !prev;
            saveDarkModeToLocalStorage(next);
            // Notify other hook instances in the same tab
            window.dispatchEvent(new Event(DARK_MODE_CHANGE_EVENT));
            return next;
        });
    }, []);

    // Sync with other hook instances when they toggle dark mode
    useEffect(() => {
        const syncHandler = () => {
            setIsDarkMode(loadDarkModeFromLocalStorage());
        };
        window.addEventListener(DARK_MODE_CHANGE_EVENT, syncHandler);
        return () => window.removeEventListener(DARK_MODE_CHANGE_EVENT, syncHandler);
    }, []);

    // Listen for OS-level theme changes when the user hasn't explicitly set a preference
    useEffect(() => {
        const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
        const handler = (e: MediaQueryListEvent) => {
            if (localStorage.getItem(DARK_MODE_KEY) === null) {
                setIsDarkMode(e.matches);
            }
        };
        mediaQuery.addEventListener('change', handler);
        return () => mediaQuery.removeEventListener('change', handler);
    }, []);

    return [isDarkMode, toggleDarkMode];
}

/**
 * Reads the dark mode preference from localStorage without React state.
 * Used for initial theme selection before hooks are available.
 */
export function loadIsDarkMode(): boolean {
    return loadDarkModeFromLocalStorage();
}
