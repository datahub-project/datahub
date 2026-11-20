import React, { useMemo } from 'react';

import ActorPoliciesTable from '@app/permissions/policy/actorPolicies/ActorPoliciesTable';
import {
    PolicyFieldMatch,
    buildActorPolicyRows,
    isTruncated,
    listPoliciesMatchingAny,
} from '@app/permissions/policy/actorPolicies/actorPolicies.utils';

import { useGetGroupRoleUrnsQuery } from '@graphql/actorPolicies.generated';
import { useListPoliciesQuery } from '@graphql/policy.generated';

/**
 * Policies that name the group, apply to all groups, or name one of the group's roles.
 *
 * Policies granted to resource owners aren't listed: `resourceOwners` isn't a search criterion that can be tied to a
 * group. All-users policies are left out on purpose, since they aren't specific to the group.
 */
export default function GroupPoliciesTab({ urn }: { urn: string }) {
    const roles = useGetGroupRoleUrnsQuery({ variables: { urn } });
    const roleUrns = useMemo(
        () =>
            roles.data?.corpGroup?.roles?.relationships
                ?.map((relationship) => relationship.entity?.urn)
                .filter((roleUrn): roleUrn is string => !!roleUrn) ?? [],
        [roles.data],
    );

    const matches: PolicyFieldMatch[] = [
        { field: 'groups', values: [urn] },
        { field: 'allGroups', values: ['true'] },
        ...(roleUrns.length > 0 ? [{ field: 'roles', values: roleUrns }] : []),
    ];
    // Waits for the roles so the policy search runs once, with the roles branch included.
    const policies = useListPoliciesQuery({ variables: listPoliciesMatchingAny(matches), skip: roles.loading });

    const rows = buildActorPolicyRows(policies.data?.listPolicies?.policies ?? [], {
        actorType: 'group',
        actorUrn: urn,
        groupUrns: [],
        roleUrns,
    });

    return (
        <ActorPoliciesTable
            rows={rows}
            loading={roles.loading || policies.loading}
            error={!!roles.error || !!policies.error}
            truncated={isTruncated(policies.data?.listPolicies)}
        />
    );
}
