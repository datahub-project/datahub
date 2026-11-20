import { Alert, Pill, SimpleSelect, Table, Text } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import PolicyDetailsModal from '@app/permissions/policy/PolicyDetailsModal';
import { PolicyStatePill, PolicyTypePill } from '@app/permissions/policy/PolicyPills';
import {
    ActorPolicyRow,
    ListedPolicy,
    PolicyReason,
    PolicyStateFilter,
} from '@app/permissions/policy/actorPolicies/actorPolicies.types';
import { ACTOR_POLICIES_PAGE_SIZE, filterRowsByState } from '@app/permissions/policy/actorPolicies/actorPolicies.utils';
import getPolicyPrivilegeNames from '@app/permissions/policy/getPolicyPrivilegeNames';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { useGetPolicyPrivilegesQuery } from '@graphql/app.generated';
import { EntityType, Policy } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 16px;
`;

const Toolbar = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const ManageLink = styled(Link)`
    margin-left: auto;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

const PolicyName = styled.span`
    font-weight: 700;
    cursor: pointer;
`;

const Reasons = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const EmptyContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 40px;
`;

type Props = {
    rows: ActorPolicyRow[];
    loading: boolean;
    error?: boolean;
    /** Some policies were left out because a query returned only its first page. */
    truncated?: boolean;
    /** Shown under the table, e.g. to say what the list can't include. */
    note?: string;
};

function ReasonPill({ reason, policy }: { reason: PolicyReason; policy: ListedPolicy }) {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();
    const pill = (label: string) => <Pill label={label} variant="outline" color="gray" size="sm" clickable={false} />;

    switch (reason.type) {
        case 'direct':
            return pill(t('actorPolicies.reason.direct'));
        case 'everyone':
            return pill(t('actorPolicies.reason.everyone'));
        case 'allGroups':
            return pill(t('actorPolicies.reason.allGroups'));
        case 'role': {
            const role = policy.actors.resolvedRoles?.find((r) => r.urn === reason.urn);
            return pill(t('actorPolicies.reason.role', { name: role?.name || reason.urn }));
        }
        case 'group': {
            const group = policy.actors.resolvedGroups?.find((g) => g.urn === reason.urn);
            const name = group ? entityRegistry.getDisplayName(EntityType.CorpGroup, group) : reason.urn;
            return (
                <Link
                    to={entityRegistry.getEntityUrl(EntityType.CorpGroup, reason.urn)}
                    onClick={(e) => e.stopPropagation()}
                >
                    {pill(t('actorPolicies.reason.group', { name }))}
                </Link>
            );
        }
        default:
            return null;
    }
}

/** Read-only list of the policies that apply to a user or group, styled like Settings > Policies. */
export default function ActorPoliciesTable({ rows, loading, error, truncated, note }: Props) {
    const { t } = useTranslation('settings.permissions');
    const [status, setStatus] = useState<PolicyStateFilter>('ACTIVE');
    const [focusPolicy, setFocusPolicy] = useState<ListedPolicy | undefined>(undefined);
    const { data: privilegesData } = useGetPolicyPrivilegesQuery({ fetchPolicy: 'cache-first' });
    const policyPrivileges = privilegesData?.appConfig?.policiesConfig;

    const visibleRows = filterRowsByState(rows, status);
    const statusOptions = [
        { value: 'ALL', label: t('statusAll') },
        { value: 'ACTIVE', label: t('statusActive') },
        { value: 'INACTIVE', label: t('statusInactive') },
    ];

    const columns = [
        {
            title: t('column.name'),
            key: 'name',
            width: '25%',
            render: (row: ActorPolicyRow) => <PolicyName>{row.policy.name}</PolicyName>,
        },
        {
            title: t('column.type'),
            key: 'type',
            width: '12%',
            render: (row: ActorPolicyRow) => <PolicyTypePill type={row.policy.type} />,
        },
        {
            title: t('actorPolicies.column.appliesBecause'),
            key: 'reasons',
            render: (row: ActorPolicyRow) => (
                <Reasons>
                    {row.reasons.map((reason) => (
                        <ReasonPill
                            key={`${reason.type}-${'urn' in reason ? reason.urn : ''}`}
                            reason={reason}
                            policy={row.policy}
                        />
                    ))}
                </Reasons>
            ),
        },
        {
            title: t('column.state'),
            key: 'state',
            width: '12%',
            render: (row: ActorPolicyRow) => <PolicyStatePill state={row.policy.state} />,
        },
    ];

    return (
        <Container data-testid="actor-policies">
            <Toolbar>
                <SimpleSelect
                    options={statusOptions}
                    values={[status]}
                    onUpdate={(values) => setStatus((values[0] as PolicyStateFilter) || 'ALL')}
                    showClear={false}
                    width={120}
                    minWidth="120px"
                    position="start"
                    size="md"
                    dataTestId="actor-policies-state-filter"
                />
                {!loading && <Text size="sm">{t('actorPolicies.total', { count: visibleRows.length })}</Text>}
                <ManageLink to={PageRoutes.SETTINGS_POLICIES}>{t('actorPolicies.managePolicies')}</ManageLink>
            </Toolbar>
            {error && <Alert variant="error" title={t('loadError')} />}
            {truncated && (
                <Alert variant="warning" title={t('actorPolicies.truncated', { count: ACTOR_POLICIES_PAGE_SIZE })} />
            )}
            {!loading && visibleRows.length === 0 ? (
                !error && (
                    <EmptyContainer>
                        <Text size="md">{t('emptyPolicies')}</Text>
                    </EmptyContainer>
                )
            ) : (
                <Table
                    columns={columns}
                    data={visibleRows}
                    rowKey="urn"
                    isLoading={loading}
                    style={{ tableLayout: 'fixed' }}
                    onRowClick={(row) => setFocusPolicy(row.policy)}
                />
            )}
            {note && <Alert variant="info" title={note} />}
            {focusPolicy && (
                <PolicyDetailsModal
                    // The listed policy omits fields of the full Policy type that the modal doesn't read.
                    policy={focusPolicy as Policy}
                    open
                    onClose={() => setFocusPolicy(undefined)}
                    privileges={getPolicyPrivilegeNames(focusPolicy, policyPrivileges)}
                    resourcePrivileges={policyPrivileges?.resourcePrivileges}
                />
            )}
        </Container>
    );
}
