import { act, renderHook } from '@testing-library/react-hooks';

import { usePolicy } from '@app/permissions/policy/usePolicy';

import { PolicyEffect, PolicyState, PolicyType } from '@types';

const mockReadQuery = vi.fn().mockReturnValue({
    listPolicies: { start: 0, count: 10, total: 0, policies: [] },
});
const mockWriteQuery = vi.fn();

vi.mock('@apollo/client', async () => ({
    ...((await vi.importActual('@apollo/client')) as any),
    useApolloClient: () => ({
        readQuery: mockReadQuery,
        writeQuery: mockWriteQuery,
    }),
}));

const mockCreatePolicy = vi.fn().mockResolvedValue({ data: { createPolicy: 'urn:li:datahubPolicy:new' } });
const mockUpdatePolicy = vi.fn().mockResolvedValue({ data: { updatePolicy: 'urn:li:datahubPolicy:existing' } });
const mockDeletePolicy = vi.fn().mockResolvedValue({});

vi.mock('@graphql/policy.generated', async (importOriginal) => {
    const actual = (await importOriginal()) as Record<string, unknown>;
    return {
        ...actual,
        useCreatePolicyMutation: () => [mockCreatePolicy, { error: undefined }],
        useUpdatePolicyMutation: () => [mockUpdatePolicy, { error: undefined }],
        useDeletePolicyMutation: () => [mockDeletePolicy, { error: undefined }],
    };
});

vi.mock('@components', () => ({
    toast: { success: vi.fn(), error: vi.fn() },
}));

const basePolicyActors = {
    users: [],
    groups: [],
    allUsers: false,
    allGroups: false,
    resourceOwners: false,
    resourceOwnersTypes: null,
};

const denyPolicy = {
    type: PolicyType.Metadata,
    name: 'Deny Test Policy',
    state: PolicyState.Active,
    effect: PolicyEffect.Deny,
    description: 'Denies access',
    privileges: ['EDIT_ENTITY_TAGS'],
    resources: null,
    actors: basePolicyActors,
};

const allowPolicy = {
    ...denyPolicy,
    name: 'Allow Test Policy',
    effect: PolicyEffect.Allow,
};

function renderUsePolicy(focusPolicyUrn?: string) {
    return renderHook(() => usePolicy({}, focusPolicyUrn, vi.fn(), vi.fn(), vi.fn(), vi.fn()));
}

describe('usePolicy - effect handling in toPolicyInput', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockReadQuery.mockReturnValue({
            listPolicies: { start: 0, count: 10, total: 0, policies: [] },
        });
    });

    it('sends effect=DENY when creating a Deny policy', async () => {
        const { result } = renderUsePolicy();

        await act(async () => {
            result.current.onSavePolicy(denyPolicy);
        });

        expect(mockCreatePolicy).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ effect: PolicyEffect.Deny }),
                }),
            }),
        );
    });

    it('sends effect=ALLOW when creating an Allow policy', async () => {
        const { result } = renderUsePolicy();

        await act(async () => {
            result.current.onSavePolicy(allowPolicy);
        });

        expect(mockCreatePolicy).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ effect: PolicyEffect.Allow }),
                }),
            }),
        );
    });

    it('sends effect=DENY when updating an existing Deny policy', async () => {
        const { result } = renderUsePolicy('urn:li:datahubPolicy:existing');

        await act(async () => {
            result.current.onSavePolicy(denyPolicy);
        });

        expect(mockUpdatePolicy).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    urn: 'urn:li:datahubPolicy:existing',
                    input: expect.objectContaining({ effect: PolicyEffect.Deny }),
                }),
            }),
        );
    });

    it('defaults to effect=ALLOW when effect is undefined (backward-compat with pre-feature policies)', async () => {
        const policyWithoutEffect = { ...denyPolicy, effect: undefined as any };
        const { result } = renderUsePolicy();

        await act(async () => {
            result.current.onSavePolicy(policyWithoutEffect);
        });

        expect(mockCreatePolicy).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ effect: PolicyEffect.Allow }),
                }),
            }),
        );
    });
});
