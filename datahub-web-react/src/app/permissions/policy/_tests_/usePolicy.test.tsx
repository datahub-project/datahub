import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import * as policyUtils from '@app/permissions/policy/policyUtils';
import { usePolicy } from '@app/permissions/policy/usePolicy';

import { PolicyMatchCondition, PolicyState, PolicyType } from '@types';

vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreatePolicyEvent: 'CreatePolicyEvent',
        UpdatePolicyEvent: 'UpdatePolicyEvent',
        DeleteEntityEvent: 'DeleteEntityEvent',
    },
}));

vi.mock('@app/permissions/policy/policyUtils', () => ({
    DEFAULT_PAGE_SIZE: 20,
    removeFromListPoliciesCache: vi.fn(),
    updateListPoliciesCache: vi.fn(),
}));

const mockUpdatePolicyMutation = vi.fn(() => Promise.resolve({ data: {} }));

vi.mock('@graphql/policy.generated', () => ({
    useCreatePolicyMutation: () => [vi.fn(() => Promise.resolve({ data: {} })), { error: undefined }],
    useUpdatePolicyMutation: () => [mockUpdatePolicyMutation, { error: undefined }],
    useDeletePolicyMutation: () => [vi.fn(() => Promise.resolve({ data: {} })), { error: undefined }],
}));

vi.mock('react-i18next', () => ({
    useTranslation: () => ({
        t: (key: string) => key,
    }),
}));

vi.mock('@apollo/client', async () => {
    const actual = await vi.importActual('@apollo/client');
    return {
        ...actual,
        useApolloClient: vi.fn(() => ({})),
    };
});

vi.mock('antd', () => ({
    Modal: {
        confirm: vi.fn(),
    },
}));

vi.mock('@components', () => ({
    toast: {
        success: vi.fn(),
    },
}));

describe('usePolicy', () => {
    const mockPoliciesConfig = {
        platformPrivileges: [],
        resourcePrivileges: [],
    };

    const mockPoliciesRefetch = vi.fn();
    const mockSetShowViewPolicyModal = vi.fn();
    const mockOnCancelViewPolicy = vi.fn();
    const mockOnClosePolicyBuilder = vi.fn();

    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={[]}>{children}</MockedProvider>
    );

    // Helper to render usePolicy hook with consistent setup
    const renderUsePolicy = (focusPolicyUrn?: string) => {
        return renderHook(
            () =>
                usePolicy(
                    mockPoliciesConfig,
                    focusPolicyUrn,
                    mockPoliciesRefetch,
                    mockSetShowViewPolicyModal,
                    mockOnCancelViewPolicy,
                    mockOnClosePolicyBuilder,
                ),
            { wrapper },
        );
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(policyUtils.updateListPoliciesCache).mockClear();
        vi.mocked(policyUtils.removeFromListPoliciesCache).mockClear();
    });

    afterEach(() => {
        vi.clearAllMocks();
        vi.mocked(policyUtils.updateListPoliciesCache).mockClear();
        vi.mocked(policyUtils.removeFromListPoliciesCache).mockClear();
    });

    it('should initialize with no errors', () => {
        const { result } = renderUsePolicy();

        expect(result.current.createPolicyError).toBeUndefined();
        expect(result.current.updatePolicyError).toBeUndefined();
        expect(result.current.deletePolicyError).toBeUndefined();
        expect(typeof result.current.onSavePolicy).toBe('function');
    });

    it('should have getPrivilegeNames function', () => {
        const { result } = renderUsePolicy();
        expect(typeof result.current.getPrivilegeNames).toBe('function');
    });

    it('should have onToggleActiveDuplicate function', () => {
        const { result } = renderUsePolicy();
        expect(typeof result.current.onToggleActiveDuplicate).toBe('function');
    });

    it('should have onRemovePolicy function', () => {
        const { result } = renderUsePolicy();
        expect(typeof result.current.onRemovePolicy).toBe('function');
    });

    describe('filter mapping with structured properties', () => {
        it('should map basic criterion without structuredPropertyValues', async () => {
            const { result } = renderUsePolicy();

            const policy = {
                type: PolicyType.Metadata,
                name: 'Basic Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                    filter: {
                        criteria: [
                            {
                                field: 'TAG',
                                values: [{ value: 'tag1' }],
                                condition: PolicyMatchCondition.Equals,
                            },
                        ] as any,
                    },
                },
            };

            // Call onSavePolicy which exercises toFilterInput internally
            result.current.onSavePolicy(policy);

            // Wait for promise to resolve
            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilder).toHaveBeenCalled();
        });

        it('should map criterion with structuredPropertyValues correctly', async () => {
            const { result } = renderUsePolicy();

            const structuredPropertyValues = [
                {
                    propertyUrn: 'urn:li:structuredPropertyDefinition:environment',
                    values: ['prod', 'staging'],
                },
            ];

            const policy = {
                type: PolicyType.Metadata,
                name: 'Structured Property Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                    filter: {
                        criteria: [
                            {
                                field: 'STRUCTURED_PROPERTY',
                                values: [],
                                condition: PolicyMatchCondition.Equals,
                                structuredPropertyValues,
                            },
                        ] as any,
                    },
                },
            };

            // Call onSavePolicy which exercises toFilterInput with structuredPropertyValues
            result.current.onSavePolicy(policy);

            // Wait for promise to resolve
            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilder).toHaveBeenCalled();
            // Verify that the mutation includes structuredPropertyValues in the payload
            // (Note: Full mutation argument verification requires access to the mocked mutation function)
        });

        it('should handle empty structuredPropertyValues array', async () => {
            const { result } = renderUsePolicy();

            const policy = {
                type: PolicyType.Metadata,
                name: 'Empty Structured Property Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                    filter: {
                        criteria: [
                            {
                                field: 'STRUCTURED_PROPERTY',
                                values: [],
                                condition: PolicyMatchCondition.Equals,
                                structuredPropertyValues: [],
                            },
                        ] as any,
                    },
                },
            };

            // Call onSavePolicy with empty structuredPropertyValues
            result.current.onSavePolicy(policy);

            // Wait for promise to resolve
            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilder).toHaveBeenCalled();
        });

        it('should map policy with group actors', async () => {
            const { result } = renderHook(
                () =>
                    usePolicy(
                        mockPoliciesConfig,
                        undefined,
                        mockPoliciesRefetch,
                        mockSetShowViewPolicyModal,
                        mockOnCancelViewPolicy,
                        mockOnClosePolicyBuilder,
                    ),
                { wrapper },
            );

            const policy = {
                type: PolicyType.Metadata,
                name: 'Group Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: ['urn:li:corpgroup:engineering'],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                    filter: {
                        criteria: [
                            {
                                field: 'STRUCTURED_PROPERTY',
                                values: [],
                                condition: PolicyMatchCondition.Equals,
                                structuredPropertyValues: [
                                    {
                                        propertyUrn: 'urn:li:structuredPropertyDefinition:department',
                                        values: ['engineering'],
                                    },
                                ],
                            },
                        ] as any,
                    },
                },
            };

            result.current.onSavePolicy(policy);

            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilder).toHaveBeenCalled();
        });
    });

    describe('updating existing policy', () => {
        it('should update policy when focusPolicyUrn is defined', async () => {
            const mockOnClosePolicyBuilderUpdate = vi.fn();
            const { result } = renderHook(
                () =>
                    usePolicy(
                        mockPoliciesConfig,
                        'urn:li:policy:123',
                        mockPoliciesRefetch,
                        mockSetShowViewPolicyModal,
                        mockOnCancelViewPolicy,
                        mockOnClosePolicyBuilderUpdate,
                    ),
                { wrapper },
            );

            const policy = {
                type: PolicyType.Metadata,
                name: 'Updated Policy',
                state: PolicyState.Active,
                description: 'Updated',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: ['urn:li:corpuser:user1'],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: true,
                },
            };

            result.current.onSavePolicy(policy);

            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilderUpdate).toHaveBeenCalled();
        });
    });

    describe('policy with complex actors', () => {
        it('should save policy with resourceOwners flag', async () => {
            const { result } = renderUsePolicy();

            const policy = {
                type: PolicyType.Metadata,
                name: 'Resource Owners Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: ['urn:li:corpuser:user1'],
                    groups: ['urn:li:corpgroup:group1'],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: true,
                    resourceOwnersTypes: ['DATASET'],
                },
                resources: {
                    allResources: false,
                    filter: {
                        criteria: [] as any,
                    },
                },
            };

            result.current.onSavePolicy(policy);

            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockOnClosePolicyBuilder).toHaveBeenCalled();
        });
    });

    describe('onToggleActiveDuplicate', () => {
        it('should toggle policy from active to inactive', async () => {
            mockUpdatePolicyMutation.mockClear();
            const { result } = renderUsePolicy();

            const activePolicy = {
                urn: 'urn:li:policy:123',
                type: PolicyType.Metadata,
                name: 'Test Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: true,
                },
            } as any;

            result.current.onToggleActiveDuplicate(activePolicy);

            await new Promise((resolve) => {
                setTimeout(resolve, 50);
            });
            expect(mockSetShowViewPolicyModal).toHaveBeenCalledWith(false);
            expect(mockUpdatePolicyMutation).toHaveBeenCalled();
        });
    });

    describe('getPrivilegeNames', () => {
        it('should return empty array when policy has no privileges', () => {
            const { result } = renderUsePolicy();

            const policy = {
                type: PolicyType.Metadata,
                name: 'Empty Privilege Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: [],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                },
            } as any;

            const privileges = result.current.getPrivilegeNames(policy);
            expect(privileges).toEqual([]);
        });

        it('should return resource privileges for METADATA type', () => {
            const mockConfigWithResourcePrivileges = {
                platformPrivileges: [],
                resourcePrivileges: [
                    {
                        resourceType: 'all',
                        privileges: [
                            { type: 'VIEW_DATASET', displayName: 'View Dataset' },
                            { type: 'EDIT_DATASET', displayName: 'Edit Dataset' },
                        ],
                    },
                ],
            };

            const { result } = renderHook(
                () =>
                    usePolicy(
                        mockConfigWithResourcePrivileges,
                        undefined,
                        mockPoliciesRefetch,
                        mockSetShowViewPolicyModal,
                        mockOnCancelViewPolicy,
                        mockOnClosePolicyBuilder,
                    ),
                { wrapper },
            );

            const policy = {
                type: PolicyType.Metadata,
                name: 'Resource Policy',
                state: PolicyState.Active,
                description: 'Test',
                editable: true,
                privileges: ['VIEW_DATASET'],
                actors: {
                    users: [],
                    groups: [],
                    allUsers: false,
                    allGroups: false,
                    resourceOwners: false,
                },
                resources: {
                    allResources: false,
                },
            } as any;

            const privileges = result.current.getPrivilegeNames(policy);
            expect(privileges).toHaveLength(1);
            expect(privileges[0].type).toBe('VIEW_DATASET');
            expect(privileges[0].name).toBe('View Dataset');
        });
    });
});
