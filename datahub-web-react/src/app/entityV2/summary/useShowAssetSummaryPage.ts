import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useShowAssetSummaryPage() {
    const assetSummaryPageV1 = useFeatureFlag('assetSummaryPageV1');
    return assetSummaryPageV1;
}
