import { Avatar, Button, Modal, Pagination, Pill, SearchBar, Table, Text, Tooltip } from '@components';
import * as QueryString from 'query-string';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled, { useTheme } from 'styled-components';

import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import analytics, { EventType } from '@app/analytics';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { clearUserListCache } from '@app/identity/user/cacheUtils';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { ROLES_INTRO_ID } from '@app/onboarding/config/RolesOnboardingConfig';
import RoleDetailsModal from '@app/permissions/roles/RoleDetailsModal';
import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { CorpUser, DataHubPolicy, DataHubRole, EntityType } from '@types';

const TableScrollContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const RoleName = styled.span`
    font-weight: 700;
`;

const PageContainer = styled.div`
    width: 100%;
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding-top: 16px;
    overflow: hidden;
`;

const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const PillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const EmptyContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 40px;
`;

const DEFAULT_PAGE_SIZE = 10;

export const ManageRoles = () => {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [isBatchAddRolesModalVisible, setIsBatchAddRolesModalVisible] = useState(false);
    const [focusRole, setFocusRole] = useState<DataHubRole>();
    const [showViewRoleModal, setShowViewRoleModal] = useState(false);
    const [selectedUserUrns, setSelectedUserUrns] = useState<string[]>([]);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    // Role list paging.
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading: rolesLoading,
        error: rolesError,
        data: rolesData,
        refetch: rolesRefetch,
    } = useListRolesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const totalRoles = rolesData?.listRoles?.total || 0;
    const roles = useMemo(() => rolesData?.listRoles?.roles || [], [rolesData]);
    const onViewRole = (role: DataHubRole) => {
        setFocusRole(role);
        setShowViewRoleModal(true);
    };
    const resetRoleState = () => {
        setIsBatchAddRolesModalVisible(false);
        setShowViewRoleModal(false);
        setFocusRole(undefined);
        setSelectedUserUrns([]);
    };

    const [batchAssignRoleMutation, { client }] = useBatchAssignRoleMutation();
    // eslint-disable-next-line
    const batchAssignRole = (actorUrns: Array<string>) => {
        if (!focusRole || !focusRole.urn) {
            return;
        }
        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: focusRole?.urn,
                    actors: actorUrns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.BatchSelectUserRoleEvent,
                        roleUrn: focusRole?.urn,
                        userUrns: actorUrns,
                    });
                    showToastMessage(ToastType.SUCCESS, 'Assigned Role to users!', 2);
                    setTimeout(() => {
                        rolesRefetch();
                        clearUserListCache(client);
                    }, 3000);
                }
            })
            .catch((e) => {
                showToastMessage(ToastType.ERROR, `Failed to assign Role to users: \n ${e.message || ''}`, 3);
            })
            .finally(() => {
                resetRoleState();
            });
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const tableColumns = [
        {
            title: 'Name',
            key: 'name',
            width: '15%',
            render: (record: any) => (
                <RoleName style={{ color: record?.editable ? theme.colors.text : theme.colors.textSecondary }}>
                    {record?.name}
                </RoleName>
            ),
        },
        {
            title: 'Description',
            key: 'description',
            width: '25%',
            render: (record: any) => record?.description || '',
        },
        {
            title: 'Users',
            key: 'users',
            width: '15%',
            render: (record: any) => {
                const allUsers: AvatarItemProps[] = (record?.users || []).map((u) => ({
                    name: entityRegistry.getDisplayName(
                        u.urn?.startsWith('urn:li:corpGroup') ? EntityType.CorpGroup : EntityType.CorpUser,
                        u,
                    ),
                    imageUrl: u?.editableProperties?.pictureLink || u?.editableInfo?.pictureLink || undefined,
                    urn: u?.urn,
                    type: u.urn?.startsWith('urn:li:corpGroup') ? AvatarType.group : AvatarType.user,
                }));

                const totalUsers = record?.totalUsers || allUsers.length;

                if (!allUsers.length) {
                    return null;
                }

                if (totalUsers === 1) {
                    return (
                        <Tooltip title={allUsers[0].name}>
                            <Avatar
                                name={allUsers[0].name}
                                imageUrl={allUsers[0].imageUrl}
                                type={allUsers[0].type}
                                size="sm"
                                showInPill
                            />
                        </Tooltip>
                    );
                }

                return (
                    <AvatarStackWithHover
                        avatars={allUsers}
                        maxToShow={5}
                        size="sm"
                        showRemainingNumber
                        totalCount={totalUsers}
                        entityRegistry={entityRegistry as any}
                        title="Users"
                    />
                );
            },
        },
        {
            title: 'Policies',
            key: 'policies',
            width: '35%',
            render: (record: any) => {
                const policyList = record?.policies || [];
                if (!policyList.length) {
                    return null;
                }
                return (
                    <PillsContainer>
                        {policyList.map((policy) => (
                            <Pill
                                key={policy.urn}
                                label={policy?.name || ''}
                                variant="outline"
                                color="gray"
                                size="sm"
                                clickable={false}
                            />
                        ))}
                    </PillsContainer>
                );
            },
        },
        {
            title: '',
            key: 'actions',
            width: '10%',
            alignment: 'right' as const,
            render: (record: any) => (
                <ActionsContainer>
                    <Button
                        variant="text"
                        onClick={(e) => {
                            e.stopPropagation();
                            setIsBatchAddRolesModalVisible(true);
                            setFocusRole(record.role);
                        }}
                    >
                        Assign Users
                    </Button>
                </ActionsContainer>
            ),
        },
    ];

    const tableData = roles?.map((role) => ({
        role,
        urn: role?.urn,
        type: role?.type,
        description: role?.description,
        name: role?.name,
        users: role?.users?.relationships?.map((relationship) => relationship.entity as CorpUser),
        totalUsers: role?.users?.total || 0,
        policies: role?.policies?.relationships?.map((relationship) => relationship.entity as DataHubPolicy),
    }));

    return (
        <PageContainer>
            <OnboardingTour stepIds={[ROLES_INTRO_ID]} />
            {rolesError && showToastMessage(ToastType.ERROR, 'Failed to load roles! An unexpected error occurred.', 3)}
            <SearchBar
                placeholder="Search roles..."
                value={query || ''}
                onChange={(value) => {
                    setPage(1);
                    setQuery(value);
                }}
                width="300px"
                allowClear
            />
            {isBatchAddRolesModalVisible && (
                <Modal
                    title={`Assign ${focusRole?.name} Role to Users`}
                    onCancel={resetRoleState}
                    buttons={[
                        { text: 'Cancel', variant: 'text', onClick: resetRoleState },
                        {
                            text: 'Assign',
                            variant: 'filled',
                            disabled: selectedUserUrns.length === 0,
                            onClick: () => batchAssignRole(selectedUserUrns),
                        },
                    ]}
                >
                    <ActorsSearchSelect
                        selectedActorUrns={selectedUserUrns}
                        onUpdate={(actors) => setSelectedUserUrns(actors.map((a) => a.urn))}
                        placeholder="Search for users or groups..."
                    />
                </Modal>
            )}
            <TableScrollContainer>
                {!rolesLoading && tableData?.length === 0 ? (
                    <EmptyContainer>
                        <Text size="md" color="gray">
                            No Roles!
                        </Text>
                    </EmptyContainer>
                ) : (
                    <Table
                        columns={tableColumns}
                        data={tableData || []}
                        rowKey="urn"
                        isScrollable
                        isLoading={rolesLoading}
                        style={{ tableLayout: 'fixed' }}
                        onRowClick={(record: any) => onViewRole(record.role)}
                    />
                )}
            </TableScrollContainer>
            <PaginationContainer>
                <Pagination currentPage={page} itemsPerPage={pageSize} total={totalRoles} onPageChange={onChangePage} />
            </PaginationContainer>
            <RoleDetailsModal role={focusRole as DataHubRole} open={showViewRoleModal} onClose={resetRoleState} />
        </PageContainer>
    );
};
