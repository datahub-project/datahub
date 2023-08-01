import { useContext } from 'react';
import { AppConfigContext } from '../appConfigContext';

/**
 * Fetch an instance of AppConfig from the React context.
 */
export function useAppConfig() {
    return useContext(AppConfigContext);
}

export function useIsShowAcrylInfoEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAcrylInfo;
}

export function useIsShowAccessManagementEnabled(): boolean {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAccessManagement;
}
