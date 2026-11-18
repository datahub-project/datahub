import { PolicyPrivilegesConfig } from '@app/permissions/policy/policyTypes';

import { ActorPolicyFieldsFragment } from '@graphql/actorPolicies.generated';
import { Policy, PolicyType } from '@types';

export const privilegeDisplayName = (privilege: string, config?: PolicyPrivilegesConfig): string => {
    const all = [
        ...(config?.platformPrivileges ?? []),
        ...(config?.resourcePrivileges.find((p) => p.resourceType === 'all')?.privileges ?? []),
    ];
    return all.find((p) => p.type === privilege)?.displayName || privilege;
};

export const privilegeOptions = (policy: { type: PolicyType; privileges: string[] }, config?: PolicyPrivilegesConfig) =>
    policy.privileges.map((privilege) => ({ type: privilege, name: privilegeDisplayName(privilege, config) }));

/** The criteria fields a policy's resource filter uses; empty when it covers every resource. */
export const resourceFields = (policy: ActorPolicyFieldsFragment): string[] =>
    (policy.resources?.filter?.criteria ?? []).map((criterion) => criterion.field);

/** FULL-view rows carry the same shape the policy details modal renders. */
export const toPolicy = (policy: ActorPolicyFieldsFragment): Omit<Policy, 'urn'> =>
    ({ ...policy, editable: false }) as unknown as Omit<Policy, 'urn'>;
