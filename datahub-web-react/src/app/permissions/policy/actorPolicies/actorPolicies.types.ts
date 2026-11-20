import { ListPoliciesQuery } from '@graphql/policy.generated';

export type ListedPolicy = NonNullable<ListPoliciesQuery['listPolicies']>['policies'][number];

export type PolicyReason =
    | { type: 'direct' }
    | { type: 'group'; urn: string }
    | { type: 'role'; urn: string }
    | { type: 'everyone' }
    | { type: 'allGroups' };

export type ActorPolicyRow = {
    urn: string;
    policy: ListedPolicy;
    reasons: PolicyReason[];
};

export type PolicyStateFilter = 'ALL' | 'ACTIVE' | 'INACTIVE';
