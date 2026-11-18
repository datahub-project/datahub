import {
    ActorPolicyRow,
    ListedPolicy,
    PolicyReason,
    PolicyStateFilter,
} from '@app/permissions/policy/actorPolicies/actorPolicies.types';

import { Maybe, PlatformPrivileges } from '@types';

// ponytail: one page per query, no pagination. Raise it, or paginate, if an actor ever matches more policies.
export const ACTOR_POLICIES_PAGE_SIZE = 1000;

/** listPolicies variables for policies whose `field` matches any of `values`. */
export function listPoliciesByField(field: string, values: string[]) {
    return { input: { start: 0, count: ACTOR_POLICIES_PAGE_SIZE, orFilters: [{ and: [{ field, values }] }] } };
}

export type ActorPolicyContext = {
    actorType: 'user' | 'group';
    actorUrn: string;
    /** Groups the user is in. Empty for a group page. */
    groupUrns: string[];
    /** Roles of the actor, including roles a user gets through its groups. */
    roleUrns: string[];
};

function getPolicyReasons(actors: ListedPolicy['actors'], context: ActorPolicyContext): PolicyReason[] {
    const isUser = context.actorType === 'user';
    const reasons: PolicyReason[] = [];
    const named = isUser ? actors.users : actors.groups;
    if (named?.includes(context.actorUrn)) reasons.push({ type: 'direct' });
    if (isUser) {
        actors.groups
            ?.filter((urn) => context.groupUrns.includes(urn))
            .forEach((urn) => reasons.push({ type: 'group', urn }));
    }
    actors.roles?.filter((urn) => context.roleUrns.includes(urn)).forEach((urn) => reasons.push({ type: 'role', urn }));
    if (isUser && actors.allUsers) reasons.push({ type: 'everyone' });
    // Enforcement only matches "all groups" for an actor that is in at least one group.
    if (actors.allGroups && (!isUser || context.groupUrns.length > 0)) reasons.push({ type: 'allGroups' });
    return reasons;
}

/**
 * Pairs each policy with the reasons it applies to the actor, deduped by urn. A user policy that matched only
 * through "all groups" while the user is in no group is dropped, since it does not apply to them.
 */
export function buildActorPolicyRows(policies: ListedPolicy[], context: ActorPolicyContext): ActorPolicyRow[] {
    const seen = new Set<string>();
    return policies.flatMap((policy) => {
        if (seen.has(policy.urn)) return [];
        seen.add(policy.urn);
        const reasons = getPolicyReasons(policy.actors, context);
        const notApplicable =
            context.actorType === 'user' &&
            policy.actors.allGroups &&
            context.groupUrns.length === 0 &&
            reasons.length === 0;
        return notApplicable ? [] : [{ urn: policy.urn, policy, reasons }];
    });
}

export function filterRowsByState(rows: ActorPolicyRow[], status: PolicyStateFilter): ActorPolicyRow[] {
    return status === 'ALL' ? rows : rows.filter((row) => row.policy.state === status);
}

export function getActorPoliciesVisibility(
    privileges: Maybe<Pick<PlatformPrivileges, 'managePolicies' | 'manageIdentities'>> | undefined,
) {
    // listPolicies needs Manage Policies; debugAccess (user tab) also needs Manage Users & Groups.
    const showGroupPolicies = !!privileges?.managePolicies;
    return { showGroupPolicies, showUserPolicies: showGroupPolicies && !!privileges?.manageIdentities };
}
