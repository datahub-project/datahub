import { useEffect } from 'react';
import { useCustomTheme } from '../customThemeContext';
import { useAppConfig } from './useAppConfig';
import { useUserContext } from './context/useUserContext';

/**
 * Returns true if theme v2 should be enabled. There are 2 conditions in which theme v2 is enabled:
 *
 * 1. If the user has enabled theme v2 in their settings, theme v2 will be enabled for the user.
 * 2. If theme v2 is enabled globally, theme v2 will be enabled for the user.
 *
 * If theme v2 is enabled globally, it will take precedence over the user's settings.
 */
export function useIsThemeV2Enabled() {
    const isThemeV2EnabledGlobally = useIsThemeV2EnabledGlobally();
    const isThemeV2EnabledForUser = useIsThemeV2EnabledForUser();
    const isThemeV2Enabled = isThemeV2EnabledGlobally || isThemeV2EnabledForUser;
    // Update the global theme to v2 if the feature flag is enabled.
    const { updateTheme } = useCustomTheme();
    useEffect(() => {
        if (isThemeV2Enabled) {
            import('../conf/theme/theme_acryl_v2.config.json').then((theme) => updateTheme(theme));
        }
    }, [isThemeV2Enabled, updateTheme]);
    return isThemeV2Enabled;
}

/**
 * Returns true if Theme V2 is enabled for the current user.
 */
export function useIsThemeV2EnabledForUser() {
    const { user } = useUserContext();
    return !!user?.settings?.appearance?.showThemeV2;
}

/**
 * Returns true if Theme V2 is globally enabled, which means that it is fully production.
 */
export function useIsThemeV2EnabledGlobally() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.themeV2;
}
