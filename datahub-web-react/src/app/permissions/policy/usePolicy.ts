import { Modal, message } from 'antd';
import { useApolloClient } from '@apollo/client';
import {
    EntityType,
    Policy,
    PolicyMatchCriterionInput,
    PolicyMatchFilter,
    PolicyMatchFilterInput,
    PolicyState,
    PolicyType,
    Maybe,
    PolicyUpdateInput,
    ResourceFilterInput,
} from '../../../types.generated';
import {
    useCreatePolicyMutation,
    useDeletePolicyMutation,
    useUpdatePolicyMutation,
} from '../../../graphql/policy.generated';
import analytics, { EventType } from '../../analytics';
import { DEFAULT_PAGE_SIZE, removeFromListPoliciesCache, updateListPoliciesCache } from './policyUtils';

type PrivilegeOptionType = {
    type?: string;
    name?: Maybe<string>;
};

export function usePolicy(
    policiesConfig,
    focusPolicyUrn,
    policiesRefetch,
    setShowViewPolicyModal,
    onCancelViewPolicy,
    onClosePolicyBuilder,
) {
    const client = useApolloClient();

    // Construct privileges
    const platformPrivileges = policiesConfig?.platformPrivileges || [];
    const resourcePrivileges = policiesConfig?.resourcePrivileges || [];

    // Any time a policy is removed, edited, or created, refetch the list.
    const [createPolicy, { error: createPolicyError }] = useCreatePolicyMutation();

    const [updatePolicy, { error: updatePolicyError }] = useUpdatePolicyMutation();

    const [deletePolicy, { error: deletePolicyError }] = useDeletePolicyMutation();

    const toFilterInput = (filter: PolicyMatchFilter, state?: string | undefined): PolicyMatchFilterInput => {
        console.log({ state });
        return {
            criteria: filter.criteria?.map((criterion): PolicyMatchCriterionInput => {
                return {
                    field: criterion.field,
                    values: criterion.values.map((criterionValue) =>
                        criterion.field === 'TAG' && state !== 'TOGGLE'
                            ? (criterionValue as any)
                            : criterionValue.value,
                    ),
                    condition: criterion.condition,
                };
            }),
        };
    };

    const toPolicyInput = (policy: Omit<Policy, 'urn'>, state?: string | undefined): PolicyUpdateInput => {
        let policyInput: PolicyUpdateInput = {
            type: policy.type,
            name: policy.name,
            state: policy.state,
            description: policy.description,
            privileges: policy.privileges,
            actors: {
                users: policy.actors.users,
                groups: policy.actors.groups,
                allUsers: policy.actors.allUsers,
                allGroups: policy.actors.allGroups,
                resourceOwners: policy.actors.resourceOwners,
                resourceOwnersTypes: policy.actors.resourceOwnersTypes,
            },
        };
        if (policy.resources !== null && policy.resources !== undefined) {
            let resourceFilter: ResourceFilterInput = {
                type: policy.resources.type,
                resources: policy.resources.resources,
                allResources: policy.resources.allResources,
            };
            if (policy.resources.filter) {
                resourceFilter = { ...resourceFilter, filter: toFilterInput(policy.resources.filter, state) };
            }
            // Add the resource filters.
            policyInput = {
                ...policyInput,
                resources: resourceFilter,
            };
        }
        return policyInput;
    };

    const getPrivilegeNames = (policy: Omit<Policy, 'urn'>) => {
        let privileges: PrivilegeOptionType[] = [];
        if (policy?.type === PolicyType.Platform) {
            privileges = platformPrivileges
                .filter((platformPrivilege) => policy.privileges.includes(platformPrivilege.type))
                .map((platformPrivilege) => {
                    return { type: platformPrivilege.type, name: platformPrivilege.displayName };
                });
        } else {
            const allResourcePriviliges = resourcePrivileges.find(
                (resourcePrivilege) => resourcePrivilege.resourceType === 'all',
            );
            privileges =
                allResourcePriviliges?.privileges
                    .filter((resourcePrivilege) => policy.privileges.includes(resourcePrivilege.type))
                    .map((b) => {
                        return { type: b.type, name: b.displayName };
                    }) || [];
        }
        return privileges;
    };

    // On Delete Policy handler
    const onRemovePolicy = (policy: Policy) => {
        Modal.confirm({
            title: `Delete ${policy?.name}`,
            content: `Are you sure you want to remove policy?`,
            onOk() {
                deletePolicy({ variables: { urn: policy?.urn as string } }).then(() => {
                    // There must be a focus policy urn.
                    analytics.event({
                        type: EventType.DeleteEntityEvent,
                        entityUrn: policy?.urn,
                        entityType: EntityType.DatahubPolicy,
                    });
                    message.success('Successfully removed policy.');
                    removeFromListPoliciesCache(client, policy?.urn, DEFAULT_PAGE_SIZE);
                    setTimeout(() => {
                        policiesRefetch();
                    }, 4000);
                    onCancelViewPolicy();
                });
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    // On Activate and deactivate Policy handler
    const onToggleActiveDuplicate = (policy: Policy) => {
        const newState = policy?.state === PolicyState.Active ? PolicyState.Inactive : PolicyState.Active;
        const newPolicy = {
            ...policy,
            state: newState,
        };
        updatePolicy({
            variables: {
                urn: policy?.urn as string, // There must be a focus policy urn.
                input: toPolicyInput(newPolicy, 'TOGGLE'),
            },
        }).then(() => {
            const updatePolicies = {
                ...newPolicy,
                __typename: 'ListPoliciesResult',
            };
            updateListPoliciesCache(client, updatePolicies, DEFAULT_PAGE_SIZE);
            message.success(`Successfully ${newState === PolicyState.Active ? 'activated' : 'deactivated'} policy.`);
            setTimeout(() => {
                policiesRefetch();
            }, 4000);
        });

        setShowViewPolicyModal(false);
    };

    // On Add/Update Policy handler
    const onSavePolicy = (savePolicy: Omit<Policy, 'urn'>) => {
        if (focusPolicyUrn) {
            // If there's an URN associated with the focused policy, then we are editing an existing policy.
            updatePolicy({ variables: { urn: focusPolicyUrn, input: toPolicyInput(savePolicy) } }).then(() => {
                const newPolicy = {
                    __typename: 'ListPoliciesResult',
                    urn: focusPolicyUrn,
                    ...savePolicy,
                    resources: null,
                };
                analytics.event({
                    type: EventType.UpdatePolicyEvent,
                    policyUrn: focusPolicyUrn,
                });
                message.success('Successfully saved policy.');
                updateListPoliciesCache(client, newPolicy, DEFAULT_PAGE_SIZE);
                setTimeout(() => {
                    policiesRefetch();
                }, 1000);
                onClosePolicyBuilder();
            });
        } else {
            // If there's no URN associated with the focused policy, then we are creating.
            createPolicy({ variables: { input: toPolicyInput(savePolicy) } }).then((result) => {
                const newPolicy = {
                    __typename: 'ListPoliciesResult',
                    urn: result?.data?.createPolicy,
                    ...savePolicy,
                    type: null,
                    actors: null,
                    resources: null,
                };
                analytics.event({
                    type: EventType.CreatePolicyEvent,
                });
                message.success('Successfully saved policy.');
                setTimeout(() => {
                    policiesRefetch();
                }, 1000);
                updateListPoliciesCache(client, newPolicy, DEFAULT_PAGE_SIZE);
                onClosePolicyBuilder();
            });
        }
    };

    return {
        createPolicyError,
        updatePolicyError,
        deletePolicyError,
        onSavePolicy,
        onToggleActiveDuplicate,
        onRemovePolicy,
        getPrivilegeNames,
    };
}
