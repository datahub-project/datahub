import { useUserContext } from '@app/context/useUserContext';

const STORAGE_KEY = 'userLanguage';

export function useUserLanguage(): string | null | undefined {
    const { loaded, user } = useUserContext();
    const language = user?.settings?.locale?.language ?? null;

    if (loaded) {
        setInLocalStorage(language);
        return language;
    }

    return loadFromLocalStorage();
}

function setInLocalStorage(value: string | null) {
    const stored = localStorage.getItem(STORAGE_KEY);
    const serialized = JSON.stringify(value);
    if (stored !== serialized) {
        localStorage.setItem(STORAGE_KEY, serialized);
    }
}

function loadFromLocalStorage(): string | null | undefined {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored !== null ? JSON.parse(stored) : undefined;
}
