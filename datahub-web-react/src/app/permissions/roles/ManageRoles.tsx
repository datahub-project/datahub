import { Button, Tooltip } from '@components';
import { Empty, Pagination, Typography, message } from 'antd';
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
import RoleDetailsModal from '@app/permissions/roles/RoleDetailsModal';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
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

const PageContainer = styled.span`
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const ActionsContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: right;
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
                    message.success({
                        content: `Assigned Role to users!`,
                        duration: 2,
                    });
                    setTimeout(() => {
                        rolesRefetch();
                        clearUserListCache(client);
                    }, 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to assign Role to users: \n ${e.message || ''}`, duration: 3 });
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
            title: 'Users',
            dataIndex: 'users',
            key: 'users',
            render: (_: any, record: any) => {
                return (
                    <>
                        {(record?.users?.length && (
                            <AvatarsGroup
                                users={record?.users}
                                groups={record?.resolvedGroups}
                                entityRegistry={entityRegistry}
                                maxCount={3}
                                size={28}
                            />
                        )) || <Typography.Text type="secondary">No assigned users</Typography.Text>}
                    </>
                );
            },
        },
        {
            dataIndex: 'actions',
            key: 'actions',
            render: (_: any, record: any) => {
                return (
                    <ActionsContainer>
                        <Tooltip title={`Assign the ${record.name} role to users`}>
                            <AddUsersButton
                                variant="text"
                                onClick={() => {
                                    setIsBatchAddRolesModalVisible(true);
                                    setFocusRole(record.role);
                                }}
                            >
                                Add Users
                            </AddUsersButton>
                        </Tooltip>
                    </ActionsContainer>
                );
            },
        },
    ];

    const tableData = roles?.map((role) => ({
        role,
        urn: role?.urn,
        type: role?.type,
        description: role?.description,
        name: role?.name,
        users: role?.users?.relationships?.map((relationship) => relationship.entity as CorpUser),
        policies: role?.policies?.relationships?.map((relationship) => relationship.entity as DataHubPolicy),
    }));

    return (
        <PageContainer>
            <OnboardingTour stepIds={[ROLES_INTRO_ID]} />
            {rolesLoading && !rolesData && (
                <Message type="loading" content="Loading roles..." style={{ marginTop: '10%' }} />
            )}
            {rolesError && message.error('Failed to load roles! An unexpected error occurred.')}
            <SourceContainer>
                <TabToolbar>
                    <div />
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
                            titleText={`Assign ${focusRole?.name} Role to Users`}
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
        </PageContainer>
    );
};
