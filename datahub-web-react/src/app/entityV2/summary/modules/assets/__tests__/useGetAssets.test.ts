import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { DATA_SOURCES_MODULE_URN } from '@app/entityV2/summary/modules/assets/constants';
import { useGetApplicationAssets } from '@app/entityV2/summary/modules/assets/useGetApplicationAssets';
import { useGetAssets } from '@app/entityV2/summary/modules/assets/useGetAssets';
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

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetDomainAssets', () => ({
    useGetDomainAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetDataProductAssets', () => ({
    useGetDataProductAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetTermAssets', () => ({
    useGetTermAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetApplicationAssets', () => ({
    useGetApplicationAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetContainerAssets', () => ({
    useGetContainerAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetChartAssets', () => ({
    useGetChartAssets: vi.fn(),
}));
vi.mock('@app/entityV2/summary/modules/assets/useGetDashboardAssets', () => ({
    useGetDashboardContents: vi.fn(),
    useGetDashboardDataSources: vi.fn(),
}));

describe('useGetAssets', () => {
    const mockDomain = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 3,
        navigateToAssetsTab: vi.fn(),
    };
    const mockDataProduct = {
        loading: true,
        fetchAssets: vi.fn(),
        total: 8,
        navigateToAssetsTab: vi.fn(),
    };
    const mockTerm = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 5,
        navigateToAssetsTab: vi.fn(),
    };
    const mockApplication = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 4,
        navigateToAssetsTab: vi.fn(),
    };
    const mockContainer = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 6,
        navigateToAssetsTab: vi.fn(),
    };
    const mockChart = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 2,
        navigateToAssetsTab: vi.fn(),
    };
    const mockDashboardContents = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 7,
        navigateToAssetsTab: vi.fn(),
    };
    const mockDashboardDataSources = {
        loading: false,
        fetchAssets: vi.fn(),
        total: 9,
        navigateToAssetsTab: vi.fn(),
    };

    const setup = (entityType, moduleUrn?: string) => {
        (useEntityData as unknown as any).mockReturnValue({ entityType });
        (useGetDomainAssets as unknown as any).mockReturnValue(mockDomain);
        (useGetDataProductAssets as unknown as any).mockReturnValue(mockDataProduct);
        (useGetTermAssets as unknown as any).mockReturnValue(mockTerm);
        (useGetApplicationAssets as unknown as any).mockReturnValue(mockApplication);
        (useGetContainerAssets as unknown as any).mockReturnValue(mockContainer);
        (useGetChartAssets as unknown as any).mockReturnValue(mockChart);
        (useGetDashboardContents as unknown as any).mockReturnValue(mockDashboardContents);
        (useGetDashboardDataSources as unknown as any).mockReturnValue(mockDashboardDataSources);
        return renderHook(() => useGetAssets(moduleUrn));
    };

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return domain assets info when entity type is Domain', () => {
        const { result } = setup(EntityType.Domain);
        expect(result.current.fetchAssets).toBe(mockDomain.fetchAssets);
        expect(result.current.loading).toBe(mockDomain.loading);
        expect(result.current.total).toBe(mockDomain.total);
        expect(result.current.navigateToAssetsTab).toBe(mockDomain.navigateToAssetsTab);
    });

    it('should return data product assets info when entity type is DataProduct', () => {
        const { result } = setup(EntityType.DataProduct);
        expect(result.current.fetchAssets).toBe(mockDataProduct.fetchAssets);
        expect(result.current.loading).toBe(mockDataProduct.loading);
        expect(result.current.total).toBe(mockDataProduct.total);
        expect(result.current.navigateToAssetsTab).toBe(mockDataProduct.navigateToAssetsTab);
    });

    it('should return term assets info when entity type is GlossaryTerm', () => {
        const { result } = setup(EntityType.GlossaryTerm);
        expect(result.current.fetchAssets).toBe(mockTerm.fetchAssets);
        expect(result.current.loading).toBe(mockTerm.loading);
        expect(result.current.total).toBe(mockTerm.total);
        expect(result.current.navigateToAssetsTab).toBe(mockTerm.navigateToAssetsTab);
    });

    it('should return application assets info when entity type is Application', () => {
        const { result } = setup(EntityType.Application);
        expect(result.current.fetchAssets).toBe(mockApplication.fetchAssets);
        expect(result.current.loading).toBe(mockApplication.loading);
        expect(result.current.total).toBe(mockApplication.total);
        expect(result.current.navigateToAssetsTab).toBe(mockApplication.navigateToAssetsTab);
    });

    it('should return container assets info when entity type is Container', () => {
        const { result } = setup(EntityType.Container);
        expect(result.current.fetchAssets).toBe(mockContainer.fetchAssets);
        expect(result.current.loading).toBe(mockContainer.loading);
        expect(result.current.total).toBe(mockContainer.total);
        expect(result.current.navigateToAssetsTab).toBe(mockContainer.navigateToAssetsTab);
    });

    it('should return dashboard contents by default when entity type is Dashboard', () => {
        const { result } = setup(EntityType.Dashboard);
        expect(useGetDashboardContents).toHaveBeenCalledWith(false);
        expect(useGetDashboardDataSources).toHaveBeenCalledWith(true);
        expect(result.current.fetchAssets).toBe(mockDashboardContents.fetchAssets);
        expect(result.current.loading).toBe(mockDashboardContents.loading);
        expect(result.current.total).toBe(mockDashboardContents.total);
        expect(result.current.navigateToAssetsTab).toBe(mockDashboardContents.navigateToAssetsTab);
    });

    it('should return dashboard data sources when the data sources module is active', () => {
        const { result } = setup(EntityType.Dashboard, DATA_SOURCES_MODULE_URN);
        expect(useGetDashboardContents).toHaveBeenCalledWith(true);
        expect(useGetDashboardDataSources).toHaveBeenCalledWith(false);
        expect(result.current.fetchAssets).toBe(mockDashboardDataSources.fetchAssets);
        expect(result.current.loading).toBe(mockDashboardDataSources.loading);
        expect(result.current.total).toBe(mockDashboardDataSources.total);
        expect(result.current.navigateToAssetsTab).toBe(mockDashboardDataSources.navigateToAssetsTab);
    });

    it('should skip both dashboard hooks on non-dashboard entities', () => {
        setup(EntityType.Chart);
        expect(useGetDashboardContents).toHaveBeenCalledWith(true);
        expect(useGetDashboardDataSources).toHaveBeenCalledWith(true);
    });

    it('should return chart assets info when entity type is Chart', () => {
        const { result } = setup(EntityType.Chart);
        expect(result.current.fetchAssets).toBe(mockChart.fetchAssets);
        expect(result.current.loading).toBe(mockChart.loading);
        expect(result.current.total).toBe(mockChart.total);
        expect(result.current.navigateToAssetsTab).toBe(mockChart.navigateToAssetsTab);
    });

    it('should return undefineds when entity type is not mapped', () => {
        const { result } = setup(EntityType.Dataset);
        expect(result.current.fetchAssets).toBeUndefined();
        expect(result.current.loading).toBeUndefined();
        expect(result.current.total).toBeUndefined();
        expect(result.current.navigateToAssetsTab).toBeUndefined();
    });

    it('should return undefineds when entity type is missing', () => {
        const { result } = setup(undefined);
        expect(result.current.fetchAssets).toBeUndefined();
        expect(result.current.loading).toBeUndefined();
        expect(result.current.total).toBeUndefined();
        expect(result.current.navigateToAssetsTab).toBeUndefined();
    });
});
