import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

// Mocks
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/entityV2/summary/shared/useCanUpdateGlossaryEntity', () => ({
    useCanUpdateGlossaryEntity: vi.fn(),
}));

describe('useDocumentationPermission', () => {
    const setup = (entityDataProps, canUpdateGlossaryEntityMock) => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: entityDataProps,
        });
        (useCanUpdateGlossaryEntity as unknown as any).mockReturnValue(canUpdateGlossaryEntityMock);
        return renderHook(() => useDocumentationPermission());
    };

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return true when canEditDescription is true', () => {
        const { result } = setup({ privileges: { canEditDescription: true } }, false);
        expect(result.current).toBe(true);
    });

    it('should return true when canManageAssetSummary is true', () => {
        const { result } = setup({ privileges: { canManageAssetSummary: true } }, false);
        expect(result.current).toBe(true);
    });

    it('should return true when canUpdateGlossaryEntity is true', () => {
        const { result } = setup({ privileges: { canEditDescription: false, canManageAssetSummary: false } }, true);
        expect(result.current).toBe(true);
    });

    it('should return true when all permissions are true', () => {
        const { result } = setup({ privileges: { canEditDescription: true, canManageAssetSummary: true } }, true);
        expect(result.current).toBe(true);
    });

    it('should return true when two permissions are true (canEditDescription, canUpdateGlossaryEntity)', () => {
        const { result } = setup({ privileges: { canEditDescription: true } }, true);
        expect(result.current).toBe(true);
    });

    it('should return true when two permissions are true (canManageAssetSummary, canUpdateGlossaryEntity)', () => {
        const { result } = setup({ privileges: { canManageAssetSummary: true } }, true);
        expect(result.current).toBe(true);
    });

    it('should return false when all permissions are false', () => {
        const { result } = setup({ privileges: { canEditDescription: false, canManageAssetSummary: false } }, false);
        expect(result.current).toBe(false);
    });

    it('should return false when entityData is missing and canUpdateGlossaryEntity is false', () => {
        const { result } = setup(undefined, false);
        expect(result.current).toBe(false);
    });

    it('should return true when entityData is missing but canUpdateGlossaryEntity is true', () => {
        const { result } = setup(undefined, true);
        expect(result.current).toBe(true);
    });

    it('should return false when privileges is missing and canUpdateGlossaryEntity is false', () => {
        const { result } = setup({}, false);
        expect(result.current).toBe(false);
    });

    it('should return true when privileges is missing but canUpdateGlossaryEntity is true', () => {
        const { result } = setup({}, true);
        expect(result.current).toBe(true);
    });
});
