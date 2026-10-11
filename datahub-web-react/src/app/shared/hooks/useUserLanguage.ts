import { useUserContext } from '@app/context/useUserContext';
import { readCachedUserLanguage, writeCachedUserLanguage } from '@app/shared/hooks/userLanguageStorage';

export function useUserLanguage(): string | null | undefined {
    const { loaded, user } = useUserContext();
    const language = user?.settings?.locale?.language ?? null;

    if (loaded) {
        writeCachedUserLanguage(language);
        return language;
    }

    return readCachedUserLanguage();
}
