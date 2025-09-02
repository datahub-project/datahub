import { useAppConfig } from '@app/useAppConfig';

export function useShowAssetSummaryPage() {
    const appConfig = useAppConfig();
    return true;
    if (appConfig?.loaded) {
        return appConfig.config.featureFlags.assetSummaryPageV1;
    }
    return false;
}
