import { useEntityData } from '@app/entity/shared/EntityContext';
import { isDataSourcesModule } from '@app/entityV2/summary/modules/assets/constants';
import { useGetApplicationAssets } from '@app/entityV2/summary/modules/assets/useGetApplicationAssets';
import { useGetChartAssets } from '@app/entityV2/summary/modules/assets/useGetChartAssets';
import { useGetContainerAssets } from '@app/entityV2/summary/modules/assets/useGetContainerAssets';
import {
    useGetDashboardContents,
    useGetDashboardDataSources,
} from '@app/entityV2/summary/modules/assets/useGetDashboardAssets';
import { useGetDataProductAssets } from '@app/entityV2/summary/modules/assets/useGetDataProductAssets';
import { useGetDomainAssets } from '@app/entityV2/summary/modules/assets/useGetDomainAssets';
import { useGetTermAssets } from '@app/entityV2/summary/modules/assets/useGetTermAssets';

import { EntityType } from '@types';

const NUMBER_OF_ASSETS_TO_FETCH = 10;

export function useGetAssets(moduleUrn?: string) {
    const { entityType } = useEntityData();
    const useDataSources = entityType === EntityType.Dashboard && isDataSourcesModule(moduleUrn);

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

    const {
        loading: applicationAssetsLoading,
        fetchAssets: fetchApplicationAssets,
        total: applicationAssetsTotal,
        navigateToAssetsTab: navigateToApplicationAssetsTab,
    } = useGetApplicationAssets(NUMBER_OF_ASSETS_TO_FETCH);

    const {
        loading: containerAssetsLoading,
        fetchAssets: fetchContainerAssets,
        total: containerAssetsTotal,
        navigateToAssetsTab: navigateToContainerAssetsTab,
    } = useGetContainerAssets(NUMBER_OF_ASSETS_TO_FETCH);

    const {
        loading: dashboardContentsLoading,
        fetchAssets: fetchDashboardContents,
        total: dashboardContentsTotal,
        navigateToAssetsTab: navigateToDashboardContentsTab,
    } = useGetDashboardContents(entityType !== EntityType.Dashboard || useDataSources);

    const {
        loading: dashboardDataSourcesLoading,
        fetchAssets: fetchDashboardDataSources,
        total: dashboardDataSourcesTotal,
        navigateToAssetsTab: navigateToDashboardDataSourcesTab,
    } = useGetDashboardDataSources(entityType !== EntityType.Dashboard || !useDataSources);

    const {
        loading: chartAssetsLoading,
        fetchAssets: fetchChartAssets,
        total: chartAssetsTotal,
        navigateToAssetsTab: navigateToChartAssetsTab,
    } = useGetChartAssets();

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
        case EntityType.Application:
            fetchAssets = fetchApplicationAssets;
            loading = applicationAssetsLoading;
            total = applicationAssetsTotal;
            navigateToAssetsTab = navigateToApplicationAssetsTab;
            break;
        case EntityType.Container:
            fetchAssets = fetchContainerAssets;
            loading = containerAssetsLoading;
            total = containerAssetsTotal;
            navigateToAssetsTab = navigateToContainerAssetsTab;
            break;
        case EntityType.Dashboard:
            if (useDataSources) {
                fetchAssets = fetchDashboardDataSources;
                loading = dashboardDataSourcesLoading;
                total = dashboardDataSourcesTotal;
                navigateToAssetsTab = navigateToDashboardDataSourcesTab;
            } else {
                fetchAssets = fetchDashboardContents;
                loading = dashboardContentsLoading;
                total = dashboardContentsTotal;
                navigateToAssetsTab = navigateToDashboardContentsTab;
            }
            break;
        case EntityType.Chart:
            fetchAssets = fetchChartAssets;
            loading = chartAssetsLoading;
            total = chartAssetsTotal;
            navigateToAssetsTab = navigateToChartAssetsTab;
            break;
        default:
            break;
    }

    return { fetchAssets, loading, total, navigateToAssetsTab };
}
