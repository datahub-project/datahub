import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useShowDatasetSummaryPage() {
    const datasetSummaryPageV1 = useFeatureFlag('datasetSummaryPageV1');
    return datasetSummaryPageV1;
}
