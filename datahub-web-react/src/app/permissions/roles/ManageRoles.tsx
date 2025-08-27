import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Avatar, Button, Empty, Pagination, Typography, message } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EntityCapabilityType } from '@app/entity/Entity';
import { StyledTable } from '@app/entity/shared/components/styled/StyledTable';
import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { SearchSelectModal } from '@app/entity/shared/components/styled/search/SearchSelectModal';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { clearUserListCache } from '@app/identity/user/cacheUtils';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { ROLES_INTRO_ID } from '@app/onboarding/config/RolesOnboardingConfig';
import AvatarsGroup from '@app/permissions/AvatarsGroup';
import { CreateRoleModal } from '@app/permissions/roles/CreateRoleModal';
import { EditRoleModal } from '@app/permissions/roles/EditRoleModal';
import { RoleActorsModal } from '@app/permissions/roles/RoleActorsModal';
import RoleDetailsModal from '@app/permissions/roles/RoleDetailsModal';
import DeleteRoleConfirmation from '@app/permissions/roles/DeleteRoleConfirmation';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useBatchAssignRoleMutation, useDeleteRoleMutation } from '@graphql/mutations.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { CorpUser, DataHubPolicy, DataHubRole } from '@types';

const SourceContainer = styled.div`
    overflow: auto;
    display: flex;
    flex-direction: column;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const RoleName = styled.span`
    cursor: pointer;
    font-weight: 700;
`;

const ActionsContainer = styled.div`
    display: flex;
    justify-content: right;
`;

const EditRoleButton = styled(Button)`
    margin-right: 16px;
`;

const ManageActorsButton = styled(Button)`
    margin-right: 16px;
`;

const PageContainer = styled.span`
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const AddUsersButton = styled(Button)`
    margin-right: 16px;
`;

const DEFAULT_PAGE_SIZE = 10;

// TODO: Cleanup the styling.
export const ManageRoles = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [isBatchAddRolesModalVisible, setIsBatchAddRolesModalVisible] = useState(false);
    const [focusRole, setFocusRole] = useState<DataHubRole>();
    const [showViewRoleModal, setShowViewRoleModal] = useState(false);
    const [showCreateRoleModal, setShowCreateRoleModal] = useState(false);
    const [showEditRoleModal, setShowEditRoleModal] = useState(false);
    const [roleToEdit, setRoleToEdit] = useState<DataHubRole | null>(null);
    const [showActorsModal, setShowActorsModal] = useState(false);
    const [roleForActors, setRoleForActors] = useState<DataHubRole | null>(null);
    const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false);
    const [roleToDelete, setRoleToDelete] = useState<DataHubRole | null>(null);
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
        pollInterval: 5000, // Poll every 5 seconds for automatic refresh
        errorPolicy: 'all', // Continue polling even if there are errors
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
        setShowCreateRoleModal(false);
        setShowEditRoleModal(false);
        setShowActorsModal(false);
        setShowDeleteConfirmation(false);
        setFocusRole(undefined);
        setRoleToEdit(null);
        setRoleToDelete(null);
        setRoleForActors(null);
    };

    const [batchAssignRoleMutation, { client }] = useBatchAssignRoleMutation();
    const [deleteRole] = useDeleteRoleMutation();
    /** Assigns actors to the currently focused role */
    const batchAssignRole = (actorUrns: Array<string>) => {
        if (!focusRole?.urn) return;

        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: focusRole.urn,
                    actors: actorUrns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.BatchSelectUserRoleEvent,
                        roleUrn: focusRole.urn,
                        userUrns: actorUrns,
                    });
                    
                    message.success({
                        content: `Successfully assigned role to ${actorUrns.length} actor(s)!`,
                        duration: 2,
                    });
                    
                    // Refresh data after successful assignment
                    setTimeout(() => {
                        rolesRefetch();
                        clearUserListCache(client);
                    }, 3000);
                }
            })
            .catch((error) => {
                message.destroy();
                message.error({ 
                    content: `Failed to assign role: ${error.message || ''}`, 
                    duration: 3 
                });
            })
            .finally(() => {
                resetRoleState();
            });
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const handleCreateRole = () => {
        setShowCreateRoleModal(true);
    };

    const handleEditRole = (role: DataHubRole) => {
        setRoleToEdit(role);
        setShowEditRoleModal(true);
    };

    /**
     * Opens delete confirmation modal for custom roles
     */
    const handleDeleteRole = (role: DataHubRole) => {
        // Check if this is a bootstrap role that shouldn't be deleted
        const bootstrapRoles = ['Admin', 'Editor', 'Reader']; // Common bootstrap role names
        const isBootstrapRole = bootstrapRoles.includes(role.name) || role.urn.includes('datahub');

        if (isBootstrapRole) {
            message.error(`Cannot delete system role "${role.name}". Bootstrap roles are protected.`);
            return;
        }

        setRoleToDelete(role);
        setShowDeleteConfirmation(true);
    };

    /**
     * Handles successful role deletion from confirmation modal
     */
    const handleDeleteSuccess = () => {
        rolesRefetch(); // Refresh the roles list
        clearUserListCache(client); // Clear user cache
    };

    const handleRoleSuccess = () => {
        rolesRefetch();
    };

    /**
     * Opens the actors management modal for a role
     */
    const handleViewActors = (role: DataHubRole) => {
        setRoleForActors(role);
        setShowActorsModal(true);
    };

    /**
     * Handles successful actor management operations
     */
    const handleActorsSuccess = () => {
        rolesRefetch(); // Refresh roles data
        clearUserListCache(client); // Clear user cache
    };

    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (_, record: any) => {
                return (
                    <>
                        <RoleName
                            onClick={() => onViewRole(record.role)}
                            style={{ color: record?.editable ? '#000000' : ANTD_GRAY[8] }}
                        >
                            {record?.name}
                        </RoleName>
                    </>
                );
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description: string) => description || '',
        },
        {
            title: 'Actors',
            dataIndex: 'users',
            key: 'users',
            render: (_: any, record: any) => {
                const numberOfUsers = record?.totalUsers || 0;
                return (
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <div style={{ flex: 1 }}>
                            {(!!numberOfUsers && (
                                <>
                                    <AvatarsGroup
                                        users={record?.users
                                            ?.filter((u) => u.urn?.startsWith('urn:li:corpuser'))
                                            .slice(0, 5)}
                                        groups={record?.users
                                            ?.filter((u) => u.urn?.startsWith('urn:li:corpGroup'))
                                            .slice(0, 5)}
                                        entityRegistry={entityRegistry}
                                        maxCount={5}
                                        size={28}
                                    />
                                    {numberOfUsers > 5 && (
                                        <Avatar size={28} style={{ backgroundColor: 'rgb(204,204,204)' }}>
                                            +{numberOfUsers - 5}
                                        </Avatar>
                                    )}
                                </>
                            )) || <Typography.Text type="secondary">No assigned actors</Typography.Text>}
                        </div>
                    </div>
                );
            },
        },
        {
            title: '',
            dataIndex: 'actions',
            key: 'actions',
            render: (_: any, record: any) => {
                const isCustomRole = record?.editable;
                return (
                    <ActionsContainer>
                        <Tooltip title={`Assign the ${record.name} role to users and groups`}>
                            <AddUsersButton
                                onClick={() => {
                                    setIsBatchAddRolesModalVisible(true);
                                    setFocusRole(record.role);
                                }}
                            >
                                ADD ACTORS
                            </AddUsersButton>
                        </Tooltip>
                        <Tooltip title={`Manage users and groups assigned to the ${record.name} role`}>
                            <ManageActorsButton onClick={() => handleViewActors(record.role)}>
                                MANAGE ACTORS
                            </ManageActorsButton>
                        </Tooltip>
                        <EditRoleButton disabled={!isCustomRole} onClick={() => handleEditRole(record.role)}>
                            EDIT
                        </EditRoleButton>
                        <Button
                            disabled={!isCustomRole}
                            onClick={() => handleDeleteRole(record.role)}
                            type="text"
                            shape="circle"
                            danger
                        >
                            <DeleteOutlined />
                        </Button>
                    </ActionsContainer>
                );
            },
        },
    ];

    const tableData = roles?.map((role) => {
        // Determine if role is editable (not a bootstrap role)
        const bootstrapRoles = ['Admin', 'Editor', 'Reader'];
        const isEditable = !bootstrapRoles.includes(role.name) && !role.urn.includes('datahub');

        return {
            role,
            urn: role?.urn,
            type: role?.type,
            description: role?.description,
            name: role?.name,
            editable: isEditable,
            users: role?.users?.relationships?.map((relationship) => relationship.entity as CorpUser),
            totalUsers: role?.users?.total,
            policies: role?.policies?.relationships?.map((relationship) => relationship.entity as DataHubPolicy),
        };
    });

    return (
        <PageContainer>
            <OnboardingTour stepIds={[ROLES_INTRO_ID]} />
            {rolesLoading && !rolesData && (
                <Message type="loading" content="Loading roles..." style={{ marginTop: '10%' }} />
            )}
            {rolesError && message.error('Failed to load roles! An unexpected error occurred.')}
            <SourceContainer>
                <TabToolbar>
                    <div>
                        <Button type="text" onClick={handleCreateRole} data-testid="add-role-button">
                            <PlusOutlined /> Create new role
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search roles..."
                        hideRecommendations
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => {
                            setPage(1);
                            setQuery(q);
                        }}
                        entityRegistry={entityRegistry}
                    />
                    {isBatchAddRolesModalVisible && (
                        <SearchSelectModal
                            titleText={`Assign ${focusRole?.name} Role to Users & Groups`}
                            continueText="Add"
                            onContinue={batchAssignRole}
                            onCancel={resetRoleState}
                            fixedEntityTypes={Array.from(
                                entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.ROLES),
                            )}
                        />
                    )}
                </TabToolbar>
                <StyledTable
                    columns={tableColumns}
                    dataSource={tableData}
                    rowKey="urn"
                    locale={{
                        emptyText: <Empty description="No Roles!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    pagination={false}
                />
            </SourceContainer>
            <PaginationContainer>
                <Pagination
                    style={{ margin: 40 }}
                    current={page}
                    pageSize={pageSize}
                    total={totalRoles}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </PaginationContainer>
            <RoleDetailsModal role={focusRole as DataHubRole} open={showViewRoleModal} onClose={resetRoleState} />
            <CreateRoleModal
                visible={showCreateRoleModal}
                onClose={() => setShowCreateRoleModal(false)}
                onSuccess={handleRoleSuccess}
            />
            <EditRoleModal
                visible={showEditRoleModal}
                role={roleToEdit}
                onClose={() => setShowEditRoleModal(false)}
                onSuccess={handleRoleSuccess}
            />
            <RoleActorsModal
                visible={showActorsModal}
                role={roleForActors}
                onClose={() => setShowActorsModal(false)}
                onSuccess={handleActorsSuccess}
            />
            <DeleteRoleConfirmation
                open={showDeleteConfirmation}
                role={roleToDelete}
                onClose={() => setShowDeleteConfirmation(false)}
                onConfirm={handleDeleteSuccess}
            />
        </PageContainer>
    );
};
