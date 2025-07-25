import { useAppConfig } from '@app/useAppConfig';

/**
 * Returns whether the workflows feature is enabled.
 */
export function useIsWorkflowsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.actionWorkflowsEnabled;
}
