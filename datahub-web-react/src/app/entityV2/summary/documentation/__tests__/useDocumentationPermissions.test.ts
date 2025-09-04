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

describe('useDocumentationPermissions', () => {
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

    it('should return true when canEditDescription is true and canUpdateGlossaryEntity is false', () => {
        const { result } = setup({ privileges: { canEditDescription: true } }, false);
        expect(result.current).toBe(true);
    });

    it('should return true when canEditDescription is false and canUpdateGlossaryEntity is true', () => {
        const { result } = setup({ privileges: { canEditDescription: false } }, true);
        expect(result.current).toBe(true);
    });

    it('should return true when both canEditDescription and canUpdateGlossaryEntity are true', () => {
        const { result } = setup({ privileges: { canEditDescription: true } }, true);
        expect(result.current).toBe(true);
    });

    it('should return false when both canEditDescription and canUpdateGlossaryEntity are false', () => {
        const { result } = setup({ privileges: { canEditDescription: false } }, false);
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

    it('should return false when entityData.privileges is missing and canUpdateGlossaryEntity is false', () => {
        const { result } = setup({}, false);
        expect(result.current).toBe(false);
    });

    it('should return true when entityData.privileges is missing but canUpdateGlossaryEntity is true', () => {
        const { result } = setup({}, true);
        expect(result.current).toBe(true);
    });
});
