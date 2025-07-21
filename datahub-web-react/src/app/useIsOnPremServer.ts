import { useAppConfig } from '@app/useAppConfig';

export function useIsOnPremServer() {
    const appConfig = useAppConfig();

    return appConfig?.config?.dataHubConfig?.serverEnv === 'on-prem';
}
