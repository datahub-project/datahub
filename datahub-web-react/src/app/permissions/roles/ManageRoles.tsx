import React, { useEffect, useMemo, useState } from 'react';
import { Button, Empty, message, Pagination, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { useListRolesQuery } from '../../../graphql/role.generated';
import { Message } from '../../shared/Message';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import AvatarsGroup from '../AvatarsGroup';
import { useEntityRegistry } from '../../useEntityRegistry';
import { SearchBar } from '../../search/SearchBar';
import { SearchSelectModal } from '../../entity/shared/components/styled/search/SearchSelectModal';
import { EntityCapabilityType } from '../../entity/Entity';
import { useBatchAssignRoleMutation } from '../../../graphql/mutations.generated';
import { CorpUser, DataHubRole, DataHubPolicy } from '../../../types.generated';
import RoleDetailsModal from './RoleDetailsModal';
import analytics, { EventType } from '../../analytics';
import { ANTD_GRAY } from '../../entity/shared/constants';

const SourceContainer = styled.div``;

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
        fetchPolicy: 'no-cache',
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
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

    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
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
                    setTimeout(function () {
                        rolesRefetch();
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
                        {(record?.users.length && (
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
                                onClick={() => {
                                    setIsBatchAddRolesModalVisible(true);
                                    setFocusRole(record.role);
                                }}
                            >
                                ADD USERS
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
        users: role?.users?.relationships.map((relationship) => relationship.entity as CorpUser),
        policies: role?.policies?.relationships.map((relationship) => relationship.entity as DataHubPolicy),
    }));

    return (
        <PageContainer>
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
                        onQueryChange={(q) => setQuery(q)}
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
            <RoleDetailsModal role={focusRole as DataHubRole} visible={showViewRoleModal} onClose={resetRoleState} />
        </PageContainer>
    );
};
