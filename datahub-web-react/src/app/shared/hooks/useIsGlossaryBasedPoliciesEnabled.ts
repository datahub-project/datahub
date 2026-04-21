import { useAppConfig } from '@app/useAppConfig';

export function useIsGlossaryBasedPoliciesEnabled() {
    const { config } = useAppConfig();
    return config.featureFlags.glossaryBasedPoliciesEnabled;
}
