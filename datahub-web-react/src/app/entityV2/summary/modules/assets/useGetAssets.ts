import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetDataProductAssets } from '@app/entityV2/summary/modules/assets/useGetDataProductAssets';
import { useGetDomainAssets } from '@app/entityV2/summary/modules/assets/useGetDomainAssets';
import { useGetTermAssets } from '@app/entityV2/summary/modules/assets/useGetTermAssets';

import { EntityType } from '@types';

const NUMBER_OF_ASSETS_TO_FETCH = 10;

export function useGetAssets() {
    const { entityType } = useEntityData();

    const {
        loading: domainAssetsLoading,
        fetchAssets: fetchDomainAssets,
        total: domainAssetsTotal,
        navigateToAssetsTab: navigateToDomainAssetsTab,
    } = useGetDomainAssets(NUMBER_OF_ASSETS_TO_FETCH);
    const {
        loading: dataProductAssetsLoading,
        fetchAssets: fetchDataProductAssets,
        total: dataProductAssetsTotal,
        navigateToAssetsTab: navigateToDataProductAssetsTab,
    } = useGetDataProductAssets(NUMBER_OF_ASSETS_TO_FETCH);

    const {
        loading: termAssetsLoading,
        fetchAssets: fetchTermAssets,
        total: termAssetsTotal,
        navigateToAssetsTab: navigateToTermAssetsTab,
    } = useGetTermAssets(NUMBER_OF_ASSETS_TO_FETCH);

    let fetchAssets;
    let loading;
    let total;
    let navigateToAssetsTab;

    switch (entityType) {
        case EntityType.Domain:
            fetchAssets = fetchDomainAssets;
            loading = domainAssetsLoading;
            total = domainAssetsTotal;
            navigateToAssetsTab = navigateToDomainAssetsTab;
            break;
        case EntityType.DataProduct:
            fetchAssets = fetchDataProductAssets;
            loading = dataProductAssetsLoading;
            total = dataProductAssetsTotal;
            navigateToAssetsTab = navigateToDataProductAssetsTab;
            break;
        case EntityType.GlossaryTerm:
            fetchAssets = fetchTermAssets;
            loading = termAssetsLoading;
            total = termAssetsTotal;
            navigateToAssetsTab = navigateToTermAssetsTab;
            break;
        default:
            break;
    }

    return { fetchAssets, loading, total, navigateToAssetsTab };
}
