export const USER_LANGUAGE_STORAGE_KEY = 'userLanguage';

export function readCachedUserLanguage(): string | null | undefined {
    const stored = localStorage.getItem(USER_LANGUAGE_STORAGE_KEY);
    return stored !== null ? JSON.parse(stored) : undefined;
}

export function writeCachedUserLanguage(value: string | null): void {
    const stored = localStorage.getItem(USER_LANGUAGE_STORAGE_KEY);
    const serialized = JSON.stringify(value);
    if (stored !== serialized) {
        localStorage.setItem(USER_LANGUAGE_STORAGE_KEY, serialized);
    }
}
