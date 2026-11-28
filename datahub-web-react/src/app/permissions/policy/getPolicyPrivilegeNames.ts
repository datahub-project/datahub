import { PolicyPrivilegesConfig } from '@app/permissions/policy/policyTypes';

import { Maybe, Policy, PolicyType } from '@types';

/** Display names of a policy's privileges, as the policy details modal lists them. */
export default function getPolicyPrivilegeNames(
    policy: Pick<Policy, 'type' | 'privileges'>,
    policyPrivileges: PolicyPrivilegesConfig | undefined,
): { type?: string; name?: Maybe<string> }[] {
    if (policy?.type === PolicyType.Platform) {
        return (policyPrivileges?.platformPrivileges || [])
            .filter((platformPrivilege) => policy.privileges.includes(platformPrivilege.type))
            .map((platformPrivilege) => ({ type: platformPrivilege.type, name: platformPrivilege.displayName }));
    }
    const allResourcePrivileges = policyPrivileges?.resourcePrivileges?.find(
        (resourcePrivilege) => resourcePrivilege.resourceType === 'all',
    );
    return (
        allResourcePrivileges?.privileges
            .filter((resourcePrivilege) => policy.privileges.includes(resourcePrivilege.type))
            .map((b) => ({ type: b.type, name: b.displayName })) || []
    );
}
