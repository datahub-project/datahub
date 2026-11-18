import React, { useMemo } from 'react';

import ActorPoliciesTable from '@app/permissions/policy/actorPolicies/ActorPoliciesTable';
import {
    buildActorPolicyRows,
    isTruncated,
    listPoliciesByField,
} from '@app/permissions/policy/actorPolicies/actorPolicies.utils';

import { useGetGroupRoleUrnsQuery } from '@graphql/actorPolicies.generated';
import { useListPoliciesQuery } from '@graphql/policy.generated';

/**
 * Policies that name the group, apply to all groups, or name one of the group's roles. listPolicies flattens every
 * orFilters branch into a single AND (ListPoliciesResolver), so each branch is its own query and the results are
 * merged here.
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

    const byGroup = useListPoliciesQuery({ variables: listPoliciesByField('groups', [urn]) });
    const allGroups = useListPoliciesQuery({ variables: listPoliciesByField('allGroups', ['true']) });
    const byRoles = useListPoliciesQuery({
        variables: listPoliciesByField('roles', roleUrns),
        skip: roleUrns.length === 0,
    });

    const results = [byGroup, allGroups, byRoles];
    const rows = buildActorPolicyRows(
        results.flatMap((result) => result.data?.listPolicies?.policies ?? []),
        { actorType: 'group', actorUrn: urn, groupUrns: [], roleUrns },
    );

    return (
        <ActorPoliciesTable
            rows={rows}
            loading={roles.loading || results.some((result) => result.loading)}
            error={!!roles.error || results.some((result) => !!result.error)}
            truncated={isTruncated(results.map((result) => result.data?.listPolicies))}
        />
    );
}
