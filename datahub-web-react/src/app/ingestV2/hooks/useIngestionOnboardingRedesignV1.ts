import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useIngestionOnboardingRedesignV1() {
    const ingestionOnboardingRedesignV1 = useFeatureFlag('ingestionOnboardingRedesignV1');
    return ingestionOnboardingRedesignV1;
}
