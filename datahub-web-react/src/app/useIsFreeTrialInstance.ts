import { useAppConfig } from '@app/useAppConfig';

export function useIsFreeTrialInstance() {
    const appConfig = useAppConfig();

    return appConfig?.config?.dataHubConfig?.isFreeTrialInstance ?? false;
}
