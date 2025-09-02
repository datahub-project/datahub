import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetLinkPermissions } from '@app/entityV2/summary/links/useGetLinkPermissions';

import { EntityType } from '@types';

// Mocking the hooks
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

describe('useGetLinkPermissions', () => {
    const setup = (entityDataProps, userProps, entityType) => {
        (useEntityData as unknown as any).mockReturnValue({
            entityData: entityDataProps,
            entityType,
        });
        (useUserContext as unknown as any).mockReturnValue(userProps);
        return renderHook(() => useGetLinkPermissions());
    };

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return true when canEditLinks is true regardless of other permissions or entity type', () => {
        const { result } = setup(
            { privileges: { canEditLinks: true, canManageChildren: false } },
            { platformPrivileges: { manageGlossaries: false } },
            EntityType.Dataset,
        );
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryNode and user can manage glossaries', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: false } },
            { platformPrivileges: { manageGlossaries: true } },
            EntityType.GlossaryNode,
        );
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryTerm and user can manage glossaries', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: false } },
            { platformPrivileges: { manageGlossaries: true } },
            EntityType.GlossaryTerm,
        );
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryNode and canManageChildren is true', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: true } },
            { platformPrivileges: { manageGlossaries: false } },
            EntityType.GlossaryNode,
        );
        expect(result.current).toBe(true);
    });

    it('should return true when entityType is GlossaryTerm and canManageChildren is true', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: true } },
            { platformPrivileges: { manageGlossaries: false } },
            EntityType.GlossaryTerm,
        );
        expect(result.current).toBe(true);
    });

    it('should return false when none of the permissions are granted', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: false } },
            { platformPrivileges: { manageGlossaries: false } },
            EntityType.Dataset,
        );
        expect(result.current).toBe(false);
    });

    it('should return false for non-glossary entity types when only canManageChildren is true', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: true } },
            { platformPrivileges: { manageGlossaries: false } },
            EntityType.Dataset,
        );
        expect(result.current).toBe(false);
    });

    it('should return false when entityData is missing', () => {
        const { result } = setup(undefined, { platformPrivileges: { manageGlossaries: true } }, EntityType.Dataset);
        expect(result.current).toBe(false);
    });

    it('should return false when user context is missing', () => {
        const { result } = setup(
            { privileges: { canEditLinks: false, canManageChildren: false } },
            undefined,
            EntityType.GlossaryNode,
        );
        expect(result.current).toBe(false);
    });

    it('should return true when both canEditLinks and glossary permissions are true', () => {
        const { result } = setup(
            { privileges: { canEditLinks: true, canManageChildren: true } },
            { platformPrivileges: { manageGlossaries: true } },
            EntityType.GlossaryNode,
        );
        expect(result.current).toBe(true);
    });
});
