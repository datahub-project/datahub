import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useIsTemplateEditable from '@app/homeV3/context/hooks/useIsTemplateEditable';

import { PageTemplateSurfaceType } from '@types';

// Mock the dependencies
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

const mockUseEntityData = vi.mocked(useEntityData);

describe('useIsTemplateEditable', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('when templateType is AssetSummary', () => {
        it('should return true when user has canManageAssetSummary privilege', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: true,
                    },
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(true);
        });

        it('should return false when user does not have canManageAssetSummary privilege', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: false,
                    },
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(false);
        });

        it('should return false when privileges is undefined', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    // No privileges property
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(false);
        });

        it('should return false when entityData is null', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: null,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(false);
        });
    });

    describe('when templateType is HomePage', () => {
        it('should return true regardless of privileges', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: false,
                    },
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.HomePage));

            // Assert
            expect(result.current).toBe(true);
        });

        it('should return true when entityData is null', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: null,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.HomePage));

            // Assert
            expect(result.current).toBe(true);
        });

        it('should return true when privileges is undefined', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    // No privileges property
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.HomePage));

            // Assert
            expect(result.current).toBe(true);
        });
    });

    describe('memoization behavior', () => {
        it('should return the same value when dependencies have not changed', () => {
            // Arrange
            const entityData = {
                privileges: {
                    canManageAssetSummary: true,
                },
            } as any;

            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData,
                loading: false,
            });

            // Act
            const { result, rerender } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            const firstResult = result.current;

            // Rerender with same data
            rerender();
            const secondResult = result.current;

            // Assert
            expect(firstResult).toBe(secondResult);
            expect(firstResult).toBe(true);
        });

        it('should update when canManageAssetSummary privilege changes', () => {
            // Arrange
            const initialEntityData = {
                privileges: {
                    canManageAssetSummary: true,
                },
            } as any;

            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: initialEntityData,
                loading: false,
            });

            // Act
            const { result, rerender } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            expect(result.current).toBe(true);

            // Change the privilege
            const updatedEntityData = {
                privileges: {
                    canManageAssetSummary: false,
                },
            } as any;

            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: updatedEntityData,
                loading: false,
            });

            rerender();

            // Assert
            expect(result.current).toBe(false);
        });

        it('should update when templateType changes', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: false,
                    },
                } as any,
                loading: false,
            });

            // Act - Start with AssetSummary
            const { result, rerender } = renderHook(({ templateType }) => useIsTemplateEditable(templateType), {
                initialProps: { templateType: PageTemplateSurfaceType.AssetSummary },
            });

            expect(result.current).toBe(false);

            // Change to HomePage
            rerender({ templateType: PageTemplateSurfaceType.HomePage });

            // Assert
            expect(result.current).toBe(true);
        });
    });

    describe('edge cases', () => {
        it('should handle truthy values for canManageAssetSummary', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: 'truthy-string' as any,
                    },
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(true);
        });

        it('should handle falsy values for canManageAssetSummary', () => {
            // Arrange
            mockUseEntityData.mockReturnValue({
                urn: 'test-urn',
                entityType: 'DATASET' as any,
                entityData: {
                    privileges: {
                        canManageAssetSummary: 0 as any,
                    },
                } as any,
                loading: false,
            });

            // Act
            const { result } = renderHook(() => useIsTemplateEditable(PageTemplateSurfaceType.AssetSummary));

            // Assert
            expect(result.current).toBe(false);
        });
    });
});
