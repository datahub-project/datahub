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

export function useIsNestedDomainsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.nestedDomainsEnabled;
}

export function useBusinessAttributesFlag() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.businessAttributeEntityEnabled;
}

export function useIsAppConfigContextLoaded() {
    const appConfig = useAppConfig();
    return appConfig.loaded;
}
