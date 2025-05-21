import { useAppConfig } from '@app/useAppConfig';

export function useLineageV2(): boolean {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.lineageGraphV2;
}
