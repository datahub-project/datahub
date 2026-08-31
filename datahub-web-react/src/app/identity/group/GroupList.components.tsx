import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { getElasticCappedTotalValueText } from '@app/entity/shared/constants';
import type { ListGroupsGroup } from '@app/identity/group/GroupList';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Avatar, Button, Modal, Pill, Text, toast } from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { useRemoveGroupMutation } from '@graphql/group.generated';
import { CorpGroup, DataHubRole, EntityType, OriginType } from '@types';

// --- Styled components ---

export const PageContainer = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

export const GroupContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;

    table {
        table-layout: fixed;
    }
`;

export const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

export const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

export const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 12px;
`;

export const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export const PaginationContainer = styled.div`
    padding-top: 8px;
    display: flex;
    justify-content: center;
`;

const GroupInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

const GroupDetails = styled.div`
    display: flex;
    flex-direction: column;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const truncateStyle = { overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } as const;
const linkStyle = { textDecoration: 'none' } as const;

// --- Cell components ---

export const GroupNameCell = ({ group }: { group: CorpGroup }) => {
    const { t } = useTranslation('entity.identity');
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    const isExternalGroup = group.origin?.type === OriginType.External;
    const avatarUrl = group?.editableProperties?.pictureLink || undefined;

    return (
        <Link
            to={entityRegistry.getEntityUrl(EntityType.CorpGroup, group.urn)}
            style={linkStyle}
            data-testid={`group-link-${group.name}`}
        >
            <GroupInfo>
                <Avatar size="xl" name={displayName} imageUrl={avatarUrl} />
                <GroupDetails>
                    <Text size="md" weight="semiBold" lineHeight="xs">
                        {displayName}
                    </Text>
                    {isExternalGroup && (
                        <Text size="xs" color="gray">
                            {t('groups.externalBadge')}
                        </Text>
                    )}
                </GroupDetails>
            </GroupInfo>
        </Link>
    );
};

export const GroupDescriptionCell = ({ group }: { group: CorpGroup }) => {
    const description = group.editableProperties?.description || group.info?.description || '';
    return description ? (
        <Text color="gray" size="md" style={truncateStyle}>
            {description}
        </Text>
    ) : null;
};

export const GroupMembersCell = ({ group }: { group: ListGroupsGroup }) => {
    const { t } = useTranslation('entity.identity');
    const memberCount = group.memberCount?.total || 0;
    return (
        <Pill
            variant="outline"
            color="gray"
            size="sm"
            label={t('groups.membersCount', {
                count: memberCount,
                formattedCount: getElasticCappedTotalValueText(memberCount),
            })}
        />
    );
};

type GroupRoleCellProps = {
    group: ListGroupsGroup;
    selectRoleOptions: DataHubRole[];
    optimisticRoleUrn?: string;
    onRoleChange: (groupUrn: string, groupName: string, newRoleUrn: string, originalRoleUrn: string) => void;
    noRoleUrn: string;
};

export const GroupRoleCell = ({
    group,
    selectRoleOptions,
    optimisticRoleUrn,
    onRoleChange,
    noRoleUrn,
}: GroupRoleCellProps) => {
    const { t } = useTranslation('entity.identity');
    const entityRegistry = useEntityRegistry();
    const roleRelationships = group.roles?.relationships;
    const serverRole =
        roleRelationships && roleRelationships.length > 0 ? (roleRelationships[0]?.entity as DataHubRole) : undefined;
    const serverRoleUrn = serverRole?.urn || noRoleUrn;
    const currentRoleUrn = optimisticRoleUrn ?? serverRoleUrn;
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);

    return (
        <SimpleSelectRole
            selectedRole={selectRoleOptions.find((r) => r.urn === currentRoleUrn)}
            onRoleSelect={(role) => {
                const newRoleUrn = role?.urn || noRoleUrn;
                if (newRoleUrn !== currentRoleUrn) {
                    onRoleChange(group.urn, displayName, newRoleUrn, serverRoleUrn);
                }
            }}
            placeholder={t('groups.noRole')}
            size="md"
            width="fit-content"
        />
    );
};

// --- Actions menu ---

type GroupActionsMenuProps = {
    group: CorpGroup;
    onDelete: (urn: string) => void;
};

export const GroupActionsMenu = ({ group, onDelete }: GroupActionsMenuProps) => {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);
    const [removeGroupMutation] = useRemoveGroupMutation();

    const handleCopyUrn = () => {
        navigator.clipboard.writeText(group.urn);
        toast.success(t('groups.urnCopied'));
    };

    const handleDeleteConfirm = () => {
        removeGroupMutation({ variables: { urn: group.urn } })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.DeleteEntityEvent,
                        entityUrn: group.urn,
                        entityType: EntityType.CorpGroup,
                    });
                    toast.success(t('groups.deleteSuccess', { name: displayName }));
                    onDelete(group.urn);
                }
            })
            .catch((e) => {
                toast.error(t('groups.deleteError', { error: e.message || '' }));
            });
        setIsConfirmingDelete(false);
    };

    const items: ItemType[] = [
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: t('groups.copyUrn'),
            icon: Copy,
            onClick: handleCopyUrn,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            danger: true,
            onClick: () => setIsConfirmingDelete(true),
        },
    ];

    return (
        <>
            <Menu items={items}>
                <Button
                    variant="text"
                    icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'gray' }}
                    isCircle
                    data-testid={`group-menu-${displayName}`}
                />
            </Menu>
            {isConfirmingDelete && (
                <Modal
                    open={isConfirmingDelete}
                    title={t('groups.deleteTitle')}
                    onCancel={() => setIsConfirmingDelete(false)}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={() => setIsConfirmingDelete(false)}>
                                {tc('cancel')}
                            </Button>
                            <Button
                                variant="filled"
                                color="red"
                                onClick={handleDeleteConfirm}
                                data-testid="delete-group-confirm-button"
                            >
                                {tc('delete')}
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>{t('groups.deleteConfirm', { name: displayName })}</Text>
                </Modal>
            )}
        </>
    );
};
