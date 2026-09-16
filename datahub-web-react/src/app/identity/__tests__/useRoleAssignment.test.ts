import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { NO_ROLE_URN, useRoleAssignment } from '@app/identity/useRoleAssignment';

const mockMutate = vi.fn();

vi.mock('@graphql/mutations.generated', () => ({
    useBatchAssignRoleMutation: () => [mockMutate],
}));

vi.mock('@src/alchemy-components', () => ({
    toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('@app/permissions/roles/cacheUtils', () => ({
    clearRoleListCache: vi.fn(),
}));

const ADMIN_ROLE = { urn: 'urn:li:dataHubRole:Admin', name: 'Admin', type: 'DATAHUB_ROLE' as any };
const EDITOR_ROLE = { urn: 'urn:li:dataHubRole:Editor', name: 'Editor', type: 'DATAHUB_ROLE' as any };
const mockRoles = [ADMIN_ROLE, EDITOR_ROLE] as any[];

function createHookOptions(overrides = {}) {
    return {
        entityLabel: 'group',
        selectRoleOptions: mockRoles,
        refetch: vi.fn(),
        client: {} as any,
        ...overrides,
    };
}

describe('useRoleAssignment', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should initialize with null state', () => {
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        expect(result.current.roleAssignmentState).toBeNull();
    });

    it('should set state when onSelectRole is called', () => {
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });

        expect(result.current.roleAssignmentState).toEqual({
            isViewingAssignRole: true,
            actorUrn: 'urn:li:corpGroup:eng',
            actorName: 'Engineering',
            currentRoleUrn: ADMIN_ROLE.urn,
            originalRoleUrn: NO_ROLE_URN,
        });
    });

    it('should clear state on cancel', () => {
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });
        expect(result.current.roleAssignmentState).not.toBeNull();

        act(() => {
            result.current.onCancelRoleAssignment();
        });
        expect(result.current.roleAssignmentState).toBeNull();
    });

    it('should call onCancel callback with original role on cancel', () => {
        const onCancel = vi.fn();
        const { result } = renderHook(() => useRoleAssignment(createHookOptions({ onCancel })));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });

        act(() => {
            result.current.onCancelRoleAssignment();
        });

        expect(onCancel).toHaveBeenCalledWith('urn:li:corpGroup:eng', NO_ROLE_URN);
    });

    it('should not call mutation when state is null', () => {
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        act(() => {
            result.current.onConfirmRoleAssignment();
        });

        expect(mockMutate).not.toHaveBeenCalled();
    });

    it('should call mutation with correct variables on confirm', () => {
        mockMutate.mockResolvedValue({ errors: undefined });
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });

        act(() => {
            result.current.onConfirmRoleAssignment();
        });

        expect(mockMutate).toHaveBeenCalledWith({
            variables: {
                input: {
                    roleUrn: ADMIN_ROLE.urn,
                    actors: ['urn:li:corpGroup:eng'],
                },
            },
        });
    });

    it('should pass null roleUrn when assigning NoRole', () => {
        mockMutate.mockResolvedValue({ errors: undefined });
        const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', NO_ROLE_URN, ADMIN_ROLE.urn);
        });

        act(() => {
            result.current.onConfirmRoleAssignment();
        });

        expect(mockMutate).toHaveBeenCalledWith({
            variables: {
                input: {
                    roleUrn: null,
                    actors: ['urn:li:corpGroup:eng'],
                },
            },
        });
    });

    it('should call onSuccess and clear state on successful mutation', async () => {
        mockMutate.mockResolvedValue({ errors: undefined });
        const onSuccess = vi.fn();
        const { result } = renderHook(() => useRoleAssignment(createHookOptions({ onSuccess })));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });

        await act(async () => {
            result.current.onConfirmRoleAssignment();
        });

        expect(onSuccess).toHaveBeenCalledWith('urn:li:corpGroup:eng', ADMIN_ROLE.urn);
        expect(result.current.roleAssignmentState).toBeNull();
    });

    it('should call onError on failed mutation', async () => {
        mockMutate.mockRejectedValue(new Error('Network error'));
        const onError = vi.fn();
        const { result } = renderHook(() => useRoleAssignment(createHookOptions({ onError })));

        act(() => {
            result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
        });

        await act(async () => {
            result.current.onConfirmRoleAssignment();
        });

        expect(onError).toHaveBeenCalledWith('urn:li:corpGroup:eng', NO_ROLE_URN);
    });

    describe('getRoleAssignmentMessage', () => {
        it('should return empty string when state is null', () => {
            const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

            expect(result.current.getRoleAssignmentMessage()).toBe('');
        });

        it('should return assign message for a valid role', () => {
            const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

            act(() => {
                result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', ADMIN_ROLE.urn, NO_ROLE_URN);
            });

            expect(result.current.getRoleAssignmentMessage()).toBe(
                'Would you like to assign the role Admin to Engineering?',
            );
        });

        it('should return remove message for NoRole', () => {
            const { result } = renderHook(() => useRoleAssignment(createHookOptions()));

            act(() => {
                result.current.onSelectRole('urn:li:corpGroup:eng', 'Engineering', NO_ROLE_URN, ADMIN_ROLE.urn);
            });

            expect(result.current.getRoleAssignmentMessage()).toBe(
                "Would you like to remove Engineering's existing role?",
            );
        });
    });
});
