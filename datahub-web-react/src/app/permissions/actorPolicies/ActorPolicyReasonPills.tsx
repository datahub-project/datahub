import { Pill, Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { ActorPolicyFieldsFragment } from '@graphql/actorPolicies.generated';
import { EntityType, PolicyMatchReasonType } from '@types';

const Container = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
`;

const MAX_VISIBLE = 2;

type Reason = ActorPolicyFieldsFragment['matchedBy'][number];

export const OverflowPills = ({ labels }: { labels: React.ReactNode[] }) => {
    const hidden = labels.slice(MAX_VISIBLE);
    return (
        <Container>
            {labels.slice(0, MAX_VISIBLE)}
            {hidden.length > 0 && (
                <Tooltip title={<Container>{hidden}</Container>}>
                    <span>
                        <Pill label={`+${hidden.length}`} variant="outline" color="gray" size="sm" clickable={false} />
                    </span>
                </Tooltip>
            )}
        </Container>
    );
};

/** Roles have no entity page; the roles settings list filtered to the role is the closest one. */
const roleUrl = (name: string) => `${PageRoutes.SETTINGS}/permissions/roles?query=${encodeURIComponent(name)}`;

/**
 * @param linkRoles link role pills to the roles settings page, which only policy admins can open
 */
export const ActorPolicyReasonPills = ({ reasons, linkRoles }: { reasons: Reason[]; linkRoles: boolean }) => {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();

    const pill = (key: string, label: string) => (
        <Pill key={key} label={label} variant="outline" color="gray" size="sm" clickable={false} />
    );

    const groupName = (reason: Reason) =>
        reason.group ? entityRegistry.getDisplayName(EntityType.CorpGroup, reason.group) : '';

    const linked = (key: string, to: string, label: string) => (
        <Link key={key} to={to} onClick={(e) => e.stopPropagation()}>
            {pill(key, label)}
        </Link>
    );

    const rolePill = (key: string, reason: Reason, label: string) =>
        linkRoles && reason.role?.name ? linked(key, roleUrl(reason.role.name), label) : pill(key, label);

    const render = (reason: Reason, index: number) => {
        const key = `${reason.type}-${index}`;
        switch (reason.type) {
            case PolicyMatchReasonType.DirectUser:
            case PolicyMatchReasonType.DirectGroup:
                return pill(key, t('actorPolicies.reason.direct'));
            case PolicyMatchReasonType.Group:
                return linked(
                    key,
                    entityRegistry.getEntityUrl(EntityType.CorpGroup, reason.group?.urn ?? ''),
                    t('actorPolicies.reason.group', { name: groupName(reason) }),
                );
            case PolicyMatchReasonType.Role:
                return rolePill(key, reason, t('actorPolicies.reason.role', { name: reason.role?.name }));
            case PolicyMatchReasonType.RoleViaGroup:
                return rolePill(
                    key,
                    reason,
                    t('actorPolicies.reason.roleViaGroup', { role: reason.role?.name, group: groupName(reason) }),
                );
            case PolicyMatchReasonType.AllUsers:
                return pill(key, t('actorPolicies.reason.everyone'));
            case PolicyMatchReasonType.AllGroups:
                return pill(key, t('actorPolicies.reason.allGroups'));
            case PolicyMatchReasonType.ResourceOwner:
                return pill(key, t('actorPolicies.reason.owner'));
            default:
                return null;
        }
    };

    return <OverflowPills labels={reasons.map(render)} />;
};
