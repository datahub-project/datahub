import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

import { EntityType } from '@types';

// Mocks
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

describe('useCanUpdateGlossaryEntity', () => {
    const setup = (entityType, canManageGlossaries, canManageChildren) => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: { privileges: { canManageChildren } },
            entityType,
        });
        (useUserContext as unknown as any).mockReturnValue({
            platformPrivileges: { manageGlossaries: canManageGlossaries },
        });
        return renderHook(() => useCanUpdateGlossaryEntity());
    };

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return true when entityType is GlossaryNode, canManageGlossaries is true, canManageChildren is false', () => {
        const { result } = setup(EntityType.GlossaryNode, true, false);
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryNode, canManageGlossaries is false, canManageChildren is true', () => {
        const { result } = setup(EntityType.GlossaryNode, false, true);
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryTerm, canManageGlossaries is true, canManageChildren is false', () => {
        const { result } = setup(EntityType.GlossaryTerm, true, false);
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryTerm, canManageGlossaries is false, canManageChildren is true', () => {
        const { result } = setup(EntityType.GlossaryTerm, false, true);
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryTerm, both permissions are true', () => {
        const { result } = setup(EntityType.GlossaryTerm, true, true);
        expect(result.current).toBe(true);
    });

    it('should return false when entityType is GlossaryNode, both permissions are false', () => {
        const { result } = setup(EntityType.GlossaryNode, false, false);
        expect(result.current).toBe(false);
    });

    it('should return false when entityType is GlossaryTerm, both permissions are false', () => {
        const { result } = setup(EntityType.GlossaryTerm, false, false);
        expect(result.current).toBe(false);
    });

    it('should return false when entityType is Dataset, both permissions are true', () => {
        const { result } = setup(EntityType.Dataset, true, true);
        expect(result.current).toBe(false);
    });

    it('should return false when entityType is missing', () => {
        const { result } = setup(undefined, true, true);
        expect(result.current).toBe(false);
    });

    it('should return false when entityData is missing', () => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: undefined,
            entityType: EntityType.GlossaryNode,
        });
        (useUserContext as unknown as any).mockReturnValue({
            platformPrivileges: { manageGlossaries: true },
        });
        const { result } = renderHook(() => useCanUpdateGlossaryEntity());
        expect(result.current).toBe(true);
    });

    it('should return false when user context is missing', () => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: { privileges: { canManageChildren: true } },
            entityType: EntityType.GlossaryNode,
        });
        (useUserContext as unknown as any).mockReturnValue(undefined);
        const { result } = renderHook(() => useCanUpdateGlossaryEntity());
        expect(result.current).toBe(true);
    });
});
