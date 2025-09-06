import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

// Mocks
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/entityV2/summary/shared/useCanUpdateGlossaryEntity', () => ({
    useCanUpdateGlossaryEntity: vi.fn(),
}));

describe('useGetLinkPermissions', () => {
    const setup = (entityDataProps, canUpdateGlossaryEntityMock) => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: entityDataProps,
        });
        (useCanUpdateGlossaryEntity as unknown as any).mockReturnValue(canUpdateGlossaryEntityMock);
        return renderHook(() => useLinkPermission());
    };

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return true when canEditLinks is true and canUpdateGlossaryEntity is false', () => {
        const { result } = setup({ privileges: { canEditLinks: true } }, false);
        expect(result.current).toBe(true);
    });

    it('should return true when canEditLinks is false and canUpdateGlossaryEntity is true', () => {
        const { result } = setup({ privileges: { canEditLinks: false } }, true);
        expect(result.current).toBe(true);
    });

    it('should return true when both canEditLinks and canUpdateGlossaryEntity are true', () => {
        const { result } = setup({ privileges: { canEditLinks: true } }, true);
        expect(result.current).toBe(true);
    });

    it('should return false when both canEditLinks and canUpdateGlossaryEntity are false', () => {
        const { result } = setup({ privileges: { canEditLinks: false } }, false);
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
