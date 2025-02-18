import { useAppConfig } from '../../../../../../../../useAppConfig';

export const useIsContractsEnabled = () => {
    const appConfig = useAppConfig();
    const contractsEnabled = appConfig.config.featureFlags?.dataContractsEnabled;
    return contractsEnabled;
};
