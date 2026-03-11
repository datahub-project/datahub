import { message } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { getElasticCappedTotalValueText } from '@app/entity/shared/constants';
import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import {
    DEFAULT_GROUP_LIST_PAGE_SIZE,
    addGroupToListGroupsCache,
    removeGroupFromListGroupsCache,
} from '@app/identity/group/cacheUtils';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { GROUPS_INTRO_ID } from '@app/onboarding/config/GroupsOnboardingConfig';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Avatar, Button, Icon, Modal, Pagination, Pill, SearchBar, Table, Text } from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { useListGroupsQuery, useRemoveGroupMutation } from '@graphql/group.generated';
import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { CorpGroup, DataHubRole, EntityType, OriginType } from '@types';

const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

const PageContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: hidden;
`;

const GroupContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
`;

const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 12px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
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

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 60px 20px;
    text-align: center;
    gap: 16px;
`;

const ActionsButtonStyle = {
    background: 'none',
    border: 'none',
    boxShadow: 'none',
};

// --- Cell components ---

const GroupNameCell = ({ group }: { group: CorpGroup }) => {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    const isExternalGroup = group.origin?.type === OriginType.External;
    const avatarUrl = group?.editableProperties?.pictureLink || undefined;

    return (
        <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, group.urn)} style={{ textDecoration: 'none' }}>
            <GroupInfo>
                <Avatar size="xl" name={displayName} imageUrl={avatarUrl} />
                <GroupDetails>
                    <Text size="md" weight="semiBold" lineHeight="xs">
                        {displayName}
                    </Text>
                    {isExternalGroup && (
                        <Text size="xs" color="gray">
                            External
                        </Text>
                    )}
                </GroupDetails>
            </GroupInfo>
        </Link>
    );
};

const GroupDescriptionCell = ({ group }: { group: CorpGroup }) => (
    <Text color="gray" size="md">
        {group.properties?.description || '-'}
    </Text>
);

const GroupMembersCell = ({ group }: { group: CorpGroup }) => {
    const memberCount = (group as any).memberCount?.total || 0;
    return <Pill variant="filled" color="violet" label={`${getElasticCappedTotalValueText(memberCount)} members`} />;
};

type GroupRoleCellProps = {
    group: CorpGroup;
    selectRoleOptions: DataHubRole[];
    optimisticRoleUrn?: string;
    onRoleChange: (groupUrn: string, groupName: string, newRoleUrn: string, originalRoleUrn: string) => void;
};

const GroupRoleCell = ({ group, selectRoleOptions, optimisticRoleUrn, onRoleChange }: GroupRoleCellProps) => {
    const entityRegistry = useEntityRegistry();
    const castedGroup = group as any;
    const roleRelationships = castedGroup?.roles?.relationships;
    const serverRole =
        roleRelationships && roleRelationships.length > 0 && (roleRelationships[0]?.entity as DataHubRole);
    const serverRoleUrn = serverRole?.urn || NO_ROLE_URN;
    const currentRoleUrn = optimisticRoleUrn ?? serverRoleUrn;
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);

    return (
        <SimpleSelectRole
            selectedRole={selectRoleOptions.find((r) => r.urn === currentRoleUrn)}
            onRoleSelect={(role) => {
                const newRoleUrn = role?.urn || NO_ROLE_URN;
                if (newRoleUrn !== currentRoleUrn) {
                    onRoleChange(group.urn, displayName, newRoleUrn, serverRoleUrn);
                }
            }}
            placeholder="No Role"
            size="md"
            width="fit-content"
        />
    );
};

type GroupActionsMenuProps = {
    group: CorpGroup;
    onDelete: (urn: string) => void;
};

const GroupActionsMenu = ({ group, onDelete }: GroupActionsMenuProps) => {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);
    const [removeGroupMutation] = useRemoveGroupMutation();

    const handleCopyUrn = () => {
        navigator.clipboard.writeText(group.urn);
        message.success('URN copied to clipboard');
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
                    message.success(`Deleted ${displayName}!`);
                    onDelete(group.urn);
                }
            })
            .catch((e) => {
                message.error({ content: `Failed to delete: ${e.message || ''}`, duration: 3 });
            });
        setIsConfirmingDelete(false);
    };

    const items: ItemType[] = [
        {
            type: 'item' as const,
            key: 'copy-urn',
            title: 'Copy URN',
            icon: 'Copy',
            onClick: handleCopyUrn,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete',
            icon: 'Trash',
            danger: true,
            onClick: () => setIsConfirmingDelete(true),
        },
    ];

    return (
        <>
            <Menu items={items}>
                <Button
                    variant="text"
                    icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: 'xl', source: 'phosphor', color: 'gray' }}
                    isCircle
                    style={ActionsButtonStyle}
                    data-testid={`group-menu-${group.name}`}
                />
            </Menu>
            {isConfirmingDelete && (
                <Modal
                    open={isConfirmingDelete}
                    title="Delete Group"
                    onCancel={() => setIsConfirmingDelete(false)}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={() => setIsConfirmingDelete(false)}>
                                Cancel
                            </Button>
                            <Button variant="filled" color="red" onClick={handleDeleteConfirm}>
                                Delete
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>
                        Are you sure you want to delete the group &quot;{displayName}&quot;? This action cannot be
                        undone.
                    </Text>
                </Modal>
            )}
        </>
    );
};

// --- Main component ---

type GroupListProps = {
    isCreatingGroup?: boolean;
    setIsCreatingGroup?: (value: boolean) => void;
};

export const GroupList = ({
    isCreatingGroup: externalIsCreating,
    setIsCreatingGroup: externalSetIsCreating,
}: GroupListProps) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || '';

    const [query, setQuery] = useState(paramsQuery);
    const [page, setPage] = useState(1);
    const [internalIsCreating, setInternalIsCreating] = useState(false);
    const [roleAssignmentState, setRoleAssignmentState] = useState<{
        isViewingAssignRole: boolean;
        groupUrn: string;
        groupName: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null>(null);
    const [optimisticRoles, setOptimisticRoles] = useState<Record<string, string>>({});

    const isCreatingGroup = externalIsCreating ?? internalIsCreating;
    const setIsCreatingGroup = externalSetIsCreating ?? setInternalIsCreating;

    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const pageSize = DEFAULT_GROUP_LIST_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading,
        error,
        data,
        refetch: groupRefetch,
        client,
    } = useListGroupsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: query.length > 0 ? query : undefined,
            },
        },
        fetchPolicy: query.length > 0 ? 'no-cache' : 'cache-first',
    });

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: { input: { start: 0, count: 10 } },
    });

    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();

    const totalGroups = data?.listGroups?.total || 0;
    const groups = (data?.listGroups?.groups || []) as CorpGroup[];
    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeGroupFromListGroupsCache(urn, client, page, pageSize);
    };

    const handleRoleChange = (groupUrn: string, groupName: string, newRoleUrn: string, originalRoleUrn: string) => {
        setRoleAssignmentState({
            isViewingAssignRole: true,
            groupUrn,
            groupName,
            currentRoleUrn: newRoleUrn,
            originalRoleUrn,
        });
    };

    const onCancelRoleAssignment = () => {
        setRoleAssignmentState(null);
    };

    const onConfirmRoleAssignment = () => {
        if (!roleAssignmentState) return;

        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        const newRoleUrn = roleToAssign?.urn || NO_ROLE_URN;

        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: newRoleUrn === NO_ROLE_URN ? null : newRoleUrn,
                    actors: [roleAssignmentState.groupUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.SelectGroupRoleEvent,
                        roleUrn: newRoleUrn,
                        groupUrn: roleAssignmentState.groupUrn,
                    });
                    message.success(
                        newRoleUrn === NO_ROLE_URN
                            ? `Removed role from group ${roleAssignmentState.groupName}!`
                            : `Assigned role ${roleToAssign?.name} to group ${roleAssignmentState.groupName}!`,
                    );

                    const { groupUrn } = roleAssignmentState;
                    setOptimisticRoles((prev) => ({ ...prev, [groupUrn]: newRoleUrn }));
                    setRoleAssignmentState(null);

                    setTimeout(async () => {
                        clearRoleListCache(client);
                        await groupRefetch();
                        setOptimisticRoles((prev) => {
                            const updated = { ...prev };
                            delete updated[groupUrn];
                            return updated;
                        });
                    }, 3000);
                }
            })
            .catch((e) => {
                message.error(
                    newRoleUrn === NO_ROLE_URN
                        ? `Failed to remove role from group ${roleAssignmentState.groupName}: ${e.message || ''}`
                        : `Failed to assign role ${roleToAssign?.name} to group ${roleAssignmentState.groupName}: ${e.message || ''}`,
                );
            });
    };

    const getRoleAssignmentMessage = () => {
        if (!roleAssignmentState) return '';
        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        return roleToAssign?.urn === NO_ROLE_URN || !roleToAssign
            ? `Would you like to remove ${roleAssignmentState.groupName}'s existing role?`
            : `Would you like to assign the role ${roleToAssign?.name} to ${roleAssignmentState.groupName}?`;
    };

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            minWidth: '25%',
            render: (group: CorpGroup) => <GroupNameCell group={group} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            minWidth: '30%',
            render: (group: CorpGroup) => <GroupDescriptionCell group={group} />,
        },
        {
            title: 'Members',
            key: 'members',
            minWidth: '15%',
            render: (group: CorpGroup) => <GroupMembersCell group={group} />,
        },
        {
            title: 'Role',
            key: 'role',
            minWidth: '15%',
            render: (group: CorpGroup) => (
                <GroupRoleCell
                    group={group}
                    selectRoleOptions={selectRoleOptions}
                    optimisticRoleUrn={optimisticRoles[group.urn]}
                    onRoleChange={handleRoleChange}
                />
            ),
        },
        {
            title: '',
            key: 'actions',
            minWidth: '5%',
            render: (group: CorpGroup) => (
                <ActionsContainer>
                    <GroupActionsMenu group={group} onDelete={handleDelete} />
                </ActionsContainer>
            ),
        },
    ];

    return (
        <PageContainer>
            <OnboardingTour stepIds={[GROUPS_INTRO_ID]} />
            {!data && loading && <Message type="loading" content="Loading groups..." />}
            {error && <Message type="error" content="Failed to load groups! An unexpected error occurred." />}

            <GroupContainer>
                <FiltersHeader>
                    <SearchContainer>
                        <SearchBar
                            placeholder="Search groups..."
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                    </SearchContainer>
                </FiltersHeader>
            </GroupContainer>

            <TableContainer>
                {groups.length > 0 ? (
                    <>
                        <Table columns={columns} data={groups} isLoading={loading} isScrollable />
                        <div style={{ paddingTop: '8px', display: 'flex', justifyContent: 'center' }}>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={totalGroups}
                                onPageChange={onChangePage}
                            />
                        </div>
                    </>
                ) : (
                    <EmptyStateContainer>
                        {loading ? (
                            <Text size="md" color="gray">
                                Loading groups...
                            </Text>
                        ) : (
                            <>
                                <Icon icon="UsersThree" source="phosphor" size="4xl" color="gray" />
                                <Text size="md" color="gray">
                                    No groups found
                                </Text>
                                <Text size="sm" color="gray">
                                    Create a group to organize users and manage access controls
                                </Text>
                            </>
                        )}
                    </EmptyStateContainer>
                )}
            </TableContainer>

            {isCreatingGroup && (
                <CreateGroupModal
                    onClose={() => setIsCreatingGroup(false)}
                    onCreate={(group: CorpGroup) => {
                        addGroupToListGroupsCache(group, client);
                        setTimeout(() => groupRefetch(), 3000);
                    }}
                />
            )}

            {roleAssignmentState && (
                <Modal
                    open={roleAssignmentState.isViewingAssignRole}
                    title="Confirm Role Assignment"
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                Cancel
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                Confirm
                            </Button>
                        </ModalFooter>
                    }
                >
                    <Text>{getRoleAssignmentMessage()}</Text>
                </Modal>
            )}
        </PageContainer>
    );
};
