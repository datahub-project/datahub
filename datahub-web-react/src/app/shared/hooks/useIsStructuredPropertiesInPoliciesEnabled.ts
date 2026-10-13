import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useIsStructuredPropertiesInPoliciesEnabled() {
    return useFeatureFlag('structuredPropertiesInPoliciesEnabled');
}
