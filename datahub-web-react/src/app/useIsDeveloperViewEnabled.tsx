import { useUserContext } from '@app/context/useUserContext';
import { useAppConfig } from '@app/useAppConfig';

/**
 * Returns [isDeveloperViewEnabledForUser, isUserLoaded]: whether the Developer View tab is enabled for the user.
 * Defaults to false (disabled by default).
 */
export function useIsDeveloperViewEnabledForUser(): [boolean, boolean] {
    const appConfig = useAppConfig();
    const { user } = useUserContext();
    return [
        user?.settings?.appearance?.showDeveloperView ?? false,
        !!user && appConfig.loaded,
    ];
}

