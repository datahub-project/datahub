import { useAppConfig } from '@app/useAppConfig';

export function useIsFreeTrialInstance() {
    const appConfig = useAppConfig();

    return true;
}
