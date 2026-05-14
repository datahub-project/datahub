import { useAppConfig } from '@app/useAppConfig';

export function useIsMultipleDataProductsEnabled() {
    const { config } = useAppConfig();
    return config.featureFlags.multipleDataProductsPerAsset;
}
