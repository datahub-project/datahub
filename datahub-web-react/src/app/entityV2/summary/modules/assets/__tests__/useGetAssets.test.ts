import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetAssets } from '@app/entityV2/summary/modules/assets/useGetAssets';
import { useGetDataProductAssets } from '@app/entityV2/summary/modules/assets/useGetDataProductAssets';
import { useGetDomainAssets } from '@app/entityV2/summary/modules/assets/useGetDomainAssets';
import { useGetTermAssets } from '@app/entityV2/summary/modules/assets/useGetTermAssets';

import { EntityType } from '@types';

// Mock dependencies
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

    const setup = (entityType) => {
        (useEntityData as unknown as any).mockReturnValue({ entityType });
        (useGetDomainAssets as unknown as any).mockReturnValue(mockDomain);
        (useGetDataProductAssets as unknown as any).mockReturnValue(mockDataProduct);
        (useGetTermAssets as unknown as any).mockReturnValue(mockTerm);
        return renderHook(() => useGetAssets());
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
