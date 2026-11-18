import React from 'react';
import { useTranslation } from 'react-i18next';

import ActorPoliciesTable from '@app/permissions/policy/actorPolicies/ActorPoliciesTable';
import { buildActorPolicyRows, listPoliciesByField } from '@app/permissions/policy/actorPolicies/actorPolicies.utils';

import { useGetUserPolicyAccessQuery } from '@graphql/actorPolicies.generated';
import { useListPoliciesQuery } from '@graphql/policy.generated';

/**
 * Policies that apply to a user, from debugAccess (policy urns, groups and roles), loaded in full via listPolicies.
 * debugAccess also returns inactive policies, and all-groups policies for users in no group; the table filters on
 * state and buildActorPolicyRows drops the latter.
 */
export default function UserPoliciesTab({ urn }: { urn: string }) {
    const { t } = useTranslation('settings.permissions');
    const access = useGetUserPolicyAccessQuery({ variables: { urn } });
    const debugAccess = access.data?.debugAccess;
    const policyUrns = debugAccess?.policies ?? [];

    const policies = useListPoliciesQuery({
        variables: listPoliciesByField('urn', policyUrns),
        skip: policyUrns.length === 0,
    });

    const rows = buildActorPolicyRows(policies.data?.listPolicies?.policies ?? [], {
        actorType: 'user',
        actorUrn: urn,
        groupUrns: debugAccess?.groups ?? [],
        roleUrns: debugAccess?.allRoles ?? [],
    });

    return (
        <ActorPoliciesTable
            rows={rows}
            loading={access.loading || policies.loading}
            error={!!access.error || !!policies.error}
            note={t('actorPolicies.userGapNote')}
        />
    );
}
