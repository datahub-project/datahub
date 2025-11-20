import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';


import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';

// Mock the context hooks
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

describe('useDocumentPermissions', () => {
    const mockUseUserContext = vi.mocked(useUserContext);
    const mockUseEntityData = vi.mocked(useEntityData);

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return all permissions as false when no privileges are granted', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: false,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current).toEqual({
            canCreate: false,
            canEditContents: false,
            canEditTitle: false,
            canEditState: false,
            canEditType: false,
            canDelete: false,
            canMove: false,
        });
    });

    it('should allow create when user has manageDocuments privilege', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: true,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current.canCreate).toBe(true);
        expect(result.current.canDelete).toBe(true);
        expect(result.current.canMove).toBe(true);
    });

    it('should allow editing when user has canEditDescription privilege', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: false,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: true,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current.canEditContents).toBe(true);
        expect(result.current.canEditTitle).toBe(true);
        expect(result.current.canEditState).toBe(true);
        expect(result.current.canEditType).toBe(true);
    });

    it('should allow delete and move when user has canManageEntity privilege', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: false,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: true,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current.canDelete).toBe(true);
        expect(result.current.canMove).toBe(true);
        expect(result.current.canCreate).toBe(false);
    });

    it('should grant all permissions when user has all privileges', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: true,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: true,
                    canManageEntity: true,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current).toEqual({
            canCreate: true,
            canEditContents: true,
            canEditTitle: true,
            canEditState: true,
            canEditType: true,
            canDelete: true,
            canMove: true,
        });
    });

    it('should handle undefined platformPrivileges', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: undefined,
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current.canCreate).toBe(false);
        expect(result.current.canDelete).toBe(false);
        expect(result.current.canMove).toBe(false);
    });

    it('should handle undefined entity privileges', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: false,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: undefined,
            },
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current).toEqual({
            canCreate: false,
            canEditContents: false,
            canEditTitle: false,
            canEditState: false,
            canEditType: false,
            canDelete: false,
            canMove: false,
        });
    });

    it('should handle missing entityData', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: {
                manageDocuments: true,
            },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: undefined,
        } as any);

        const { result } = renderHook(() => useDocumentPermissions());

        expect(result.current.canCreate).toBe(true);
        expect(result.current.canDelete).toBe(true);
        expect(result.current.canMove).toBe(true);
        expect(result.current.canEditContents).toBe(false);
    });

    it('should memoize results and not recompute unless dependencies change', () => {
        const platformPrivileges = { manageDocuments: true };
        const entityData = {
            privileges: {
                canEditDescription: true,
                canManageEntity: true,
            },
        };

        mockUseUserContext.mockReturnValue({
            platformPrivileges,
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData,
        } as any);

        const { result, rerender } = renderHook(() => useDocumentPermissions());

        const firstResult = result.current;
        
        // Rerender without changing dependencies
        rerender();

        // Should return the same object reference (memoized)
        expect(result.current).toBe(firstResult);
    });

    it('should recompute when platformPrivileges change', () => {
        mockUseUserContext.mockReturnValue({
            platformPrivileges: { manageDocuments: false },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result, rerender } = renderHook(() => useDocumentPermissions());

        expect(result.current.canCreate).toBe(false);

        // Change platformPrivileges
        mockUseUserContext.mockReturnValue({
            platformPrivileges: { manageDocuments: true },
        } as any);

        rerender();

        expect(result.current.canCreate).toBe(true);
    });

    it('should allow delete/move with either canManageEntity OR manageDocuments', () => {
        // Test with canManageEntity only
        mockUseUserContext.mockReturnValue({
            platformPrivileges: { manageDocuments: false },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: true,
                },
            },
        } as any);

        const { result: result1 } = renderHook(() => useDocumentPermissions());
        expect(result1.current.canDelete).toBe(true);
        expect(result1.current.canMove).toBe(true);

        // Test with manageDocuments only
        mockUseUserContext.mockReturnValue({
            platformPrivileges: { manageDocuments: true },
        } as any);

        mockUseEntityData.mockReturnValue({
            entityData: {
                privileges: {
                    canEditDescription: false,
                    canManageEntity: false,
                },
            },
        } as any);

        const { result: result2 } = renderHook(() => useDocumentPermissions());
        expect(result2.current.canDelete).toBe(true);
        expect(result2.current.canMove).toBe(true);
    });
});

