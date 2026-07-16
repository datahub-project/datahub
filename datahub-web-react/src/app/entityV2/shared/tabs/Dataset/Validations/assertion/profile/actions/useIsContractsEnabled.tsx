import { useAppConfig } from '@app/useAppConfig';

export const useIsContractsEnabled = () => {
    const appConfig = useAppConfig();
    const contractsEnabled = appConfig.config.featureFlags?.dataContractsEnabled;
    return contractsEnabled;
};
