import { useAppConfig } from '@app/useAppConfig';

export function useIsDenyPoliciesEnabled() {
    const { config } = useAppConfig();
    return config.featureFlags.denyPoliciesEnabled;
}
