import { Pill, SearchBar, SimpleSelect, Table, Text, toast } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { getGraphqlErrorCode } from '@app/entityV2/shared/utils';
import { ActorPolicyReasonPills, OverflowPills } from '@app/permissions/actorPolicies/ActorPolicyReasonPills';
import { resourceScopeLabels, toPolicy } from '@app/permissions/actorPolicies/utils';
import PolicyDetailsModal from '@app/permissions/policy/PolicyDetailsModal';
import { PolicyStatePill, PolicyTypePill } from '@app/permissions/policy/policyColumns';
import { PolicyPrivilegesConfig } from '@app/permissions/policy/policyTypes';
import { getPrivilegeOptions } from '@app/permissions/policy/policyUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import {
    ActorPolicyFieldsFragment,
    useGetGroupPoliciesQuery,
    useGetUserPoliciesQuery,
} from '@graphql/actorPolicies.generated';
import { useGetPolicyPrivilegesQuery } from '@graphql/app.generated';
import { ActorPoliciesView, EntityType, PolicyState } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
`;

const Toolbar = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ManageLink = styled(Link)`
    margin-left: auto;
`;

const PolicyName = styled.span<{ $clickable: boolean }>`
    font-weight: 700;
    cursor: ${(props) => (props.$clickable ? 'pointer' : 'default')};
`;

const Cell = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
`;

const Empty = styled.div`
    display: flex;
    justify-content: center;
    padding: 40px;
`;

const OwnerSection = styled.details`
    summary {
        cursor: pointer;
        font-weight: 700;
        margin-bottom: 8px;
    }
`;

enum StatusFilter {
    ACTIVE = 'ACTIVE',
    INACTIVE = 'INACTIVE',
    ALL = 'ALL',
}

type Props = {
    urn: string;
    entityType: EntityType.CorpUser | EntityType.CorpGroup;
};

export const ActorPoliciesTab = ({ urn, entityType }: Props) => {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();
    const [query, setQuery] = useState('');
    const [status, setStatus] = useState(StatusFilter.ACTIVE);
    const [focused, setFocused] = useState<ActorPolicyFieldsFragment>();

    const isUser = entityType === EntityType.CorpUser;
    const variables = { urn, includeInactive: status !== StatusFilter.ACTIVE };
    const userResult = useGetUserPoliciesQuery({ variables, skip: !isUser });
    const groupResult = useGetGroupPoliciesQuery({ variables, skip: isUser });
    const { loading, error } = isUser ? userResult : groupResult;
    const result = isUser ? userResult.data?.corpUser?.policies : groupResult.data?.corpGroup?.policies;

    const { data: privilegesData } = useGetPolicyPrivilegesQuery({ fetchPolicy: 'cache-first' });
    const privilegesConfig: PolicyPrivilegesConfig | undefined = privilegesData?.appConfig?.policiesConfig;

    const isFull = result?.view === ActorPoliciesView.Full;

    const filterRows = (rows: ActorPolicyFieldsFragment[] = []) =>
        rows.filter(
            (p) =>
                p.name.toLowerCase().includes(query.trim().toLowerCase()) &&
                (status !== StatusFilter.INACTIVE || p.state === PolicyState.Inactive),
        );
    const policies = useMemo(() => filterRows(result?.policies), [result, query, status]); // eslint-disable-line react-hooks/exhaustive-deps
    const ownerPolicies = useMemo(() => filterRows(result?.ownerPolicies), [result, query, status]); // eslint-disable-line react-hooks/exhaustive-deps

    const pill = (key: string, label: string) => (
        <Pill key={key} label={label} variant="outline" color="gray" size="sm" clickable={false} />
    );

    const privilegePills = (p: ActorPolicyFieldsFragment) => (
        <OverflowPills
            labels={getPrivilegeOptions(
                p,
                privilegesConfig?.platformPrivileges ?? [],
                privilegesConfig?.resourcePrivileges ?? [],
            ).map((priv) => pill(priv.type, priv.name || priv.type))}
        />
    );

    const resources = (p: ActorPolicyFieldsFragment) => {
        const labels = resourceScopeLabels(p, privilegesConfig?.resourcePrivileges ?? [], (entity) =>
            entityRegistry.getDisplayName(entity.type, entity),
        );
        if (labels === null) return '—';
        if (labels.length === 0) return t('actorPolicies.allResources');
        return <OverflowPills labels={labels.map((label, i) => pill(`${label}-${i}`, label))} />;
    };

    const reasons = {
        title: t('actorPolicies.column.appliesBecause'),
        key: 'reasons',
        render: (p: ActorPolicyFieldsFragment) => <ActorPolicyReasonPills reasons={p.matchedBy} />,
    };

    const columns = isFull
        ? [
              {
                  title: t('column.name'),
                  key: 'name',
                  render: (p: ActorPolicyFieldsFragment) => (
                      <Cell>
                          <PolicyName $clickable={!p.builtIn}>{p.name}</PolicyName>
                          {p.builtIn && (
                              <Pill
                                  label={t('actorPolicies.builtIn')}
                                  variant="outline"
                                  color="gray"
                                  size="sm"
                                  clickable={false}
                              />
                          )}
                      </Cell>
                  ),
              },
              reasons,
              {
                  title: t('column.type'),
                  key: 'type',
                  render: (p: ActorPolicyFieldsFragment) => <PolicyTypePill type={p.type} />,
              },
              { title: t('actorPolicies.column.privileges'), key: 'privileges', render: privilegePills },
              { title: t('actorPolicies.column.resources'), key: 'resources', render: resources },
              {
                  title: t('column.state'),
                  key: 'state',
                  render: (p: ActorPolicyFieldsFragment) => <PolicyStatePill state={p.state} />,
              },
          ]
        : [
              {
                  title: t('actorPolicies.column.policy'),
                  key: 'name',
                  render: (p: ActorPolicyFieldsFragment) => p.name,
              },
              reasons,
              { title: t('actorPolicies.column.letsYou'), key: 'privileges', render: privilegePills },
              { title: t('actorPolicies.column.on'), key: 'resources', render: resources },
          ];

    const table = (rows: ActorPolicyFieldsFragment[]) => (
        <Table
            columns={columns}
            data={rows}
            rowKey={(p: ActorPolicyFieldsFragment) => p.urn ?? p.name}
            onRowClick={isFull ? (p: ActorPolicyFieldsFragment) => !p.builtIn && setFocused(p) : undefined}
            style={{ tableLayout: 'fixed' }}
        />
    );

    // Unauthorized (e.g. viewing another user's policies without the privilege) reads as "nothing to show";
    // any other failure is surfaced as an error.
    const unauthorized = !!error && getGraphqlErrorCode(error) === 403;
    const failed = !!error && !unauthorized;
    useEffect(() => {
        if (failed) toast.error(t('loadError'));
    }, [failed, t]);
    const hasRows = policies.length > 0 || ownerPolicies.length > 0;

    let message: string | undefined;
    if (failed) message = t('loadError');
    else if (!loading && (!hasRows || unauthorized)) message = t('actorPolicies.empty');

    return (
        <Container data-testid="actor-policies-tab">
            <Toolbar>
                <SearchBar
                    placeholder={t('searchPlaceholder')}
                    value={query}
                    onChange={setQuery}
                    width="250px"
                    allowClear
                />
                {isFull && (
                    <SimpleSelect
                        options={[
                            { value: StatusFilter.ACTIVE, label: t('statusActive') },
                            { value: StatusFilter.INACTIVE, label: t('statusInactive') },
                            { value: StatusFilter.ALL, label: t('statusAll') },
                        ]}
                        values={[status]}
                        onUpdate={(values) => setStatus((values[0] as StatusFilter) || StatusFilter.ALL)}
                        showClear={false}
                        width={120}
                        minWidth="120px"
                        size="md"
                        dataTestId="actor-policy-status-filter"
                    />
                )}
                {isFull && (
                    <ManageLink to={PageRoutes.SETTINGS_POLICIES}>{t('actorPolicies.managePolicies')}</ManageLink>
                )}
            </Toolbar>
            {message ? (
                <Empty>
                    <Text
                        size="md"
                        color={failed ? 'red' : 'gray'}
                        data-testid={failed ? 'actor-policies-error' : undefined}
                    >
                        {message}
                    </Text>
                </Empty>
            ) : (
                <>
                    {policies.length > 0 && table(policies)}
                    {ownerPolicies.length > 0 && (
                        <OwnerSection>
                            <summary>
                                {t(isUser ? 'actorPolicies.ownerSectionUser' : 'actorPolicies.ownerSectionGroup')}
                            </summary>
                            {table(ownerPolicies)}
                        </OwnerSection>
                    )}
                </>
            )}
            {focused && (
                <PolicyDetailsModal
                    policy={toPolicy(focused)}
                    open
                    onClose={() => setFocused(undefined)}
                    privileges={getPrivilegeOptions(
                        focused,
                        privilegesConfig?.platformPrivileges ?? [],
                        privilegesConfig?.resourcePrivileges ?? [],
                    )}
                    resourcePrivileges={privilegesConfig?.resourcePrivileges}
                />
            )}
        </Container>
    );
};
