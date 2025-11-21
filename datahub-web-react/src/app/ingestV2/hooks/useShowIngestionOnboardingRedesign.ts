import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useShowIngestionOnboardingRedesign() {
    const showIngestionOnboardingRedesign = useFeatureFlag('showIngestionOnboardingRedesign');
    return showIngestionOnboardingRedesign;
}
