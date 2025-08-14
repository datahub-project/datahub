import { useAppConfig } from '@app/useAppConfig';

export function useShowAssetSummaryPage() {
    const appConfig = useAppConfig();
    const showSummary = appConfig.config.featureFlags.assetSummaryPageV1;

    return showSummary;
}
