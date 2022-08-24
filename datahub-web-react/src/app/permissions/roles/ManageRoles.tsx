import React, { useEffect, useMemo, useState } from 'react';
import { Button, Empty, message, Pagination, Tooltip } from 'antd';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { useListRolesQuery } from '../../../graphql/role.generated';
import { Message } from '../../shared/Message';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { StyledTable } from '../../entity/shared/components/styled/StyledTable';
import AvatarsGroup from '../policy/AvatarsGroup';
import { useEntityRegistry } from '../../useEntityRegistry';
import { SearchBar } from '../../search/SearchBar';
import { SearchSelectModal } from '../../entity/shared/components/styled/search/SearchSelectModal';
import { EntityCapabilityType } from '../../entity/Entity';
import { useBatchAssignRoleToActorsMutation } from '../../../graphql/mutations.generated';
import { CorpUser } from '../../../types.generated';

const SourceContainer = styled.div``;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const PolicyName = styled.span`
    cursor: pointer;
    font-weight: 700;
`;

const PageContainer = styled.span`
    width: 100%;
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
    const [activeRoleInModal, setActiveRoleInModal] = useState('');
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

    const [batchAssignRoleToActorsMutation] = useBatchAssignRoleToActorsMutation();
    // eslint-disable-next-line
    const batchAssignRoleToActors = (actorUrns: Array<string>) => {
        batchAssignRoleToActorsMutation({
            variables: {
                input: {
                    roleUrn: activeRoleInModal,
                    actors: actorUrns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsBatchAddRolesModalVisible(false);
                    setActiveRoleInModal('');
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
                    <PolicyName style={{ color: record?.editable ? '#000000' : '#8C8C8C' }}>{record?.name}</PolicyName>
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
            title: 'Assignees',
            dataIndex: 'assignees',
            key: 'assignees',
            render: (_: any, record: any) => {
                return (
                    <>
                        <AvatarsGroup
                            users={record?.users}
                            groups={record?.resolvedGroups}
                            entityRegistry={entityRegistry}
                            maxCount={3}
                            size={28}
                        />
                    </>
                );
            },
        },
        {
            title: 'Assignee Count',
            dataIndex: 'assignee_count',
            key: 'assignee_count',
            render: (_: any, record: any) => record?.users.length || '',
        },
        // {
        //     title: 'Documentation',
        //     dataIndex: 'documentation',
        //     key: 'documentation',
        //     render: (_: any, record: any) => {
        //         return (
        //             <Tooltip title={`View documentation for ${record.name}`}>
        //                 <Button type="link" href={record.documentation} target="_blank" rel="noreferrer">
        //                     <LinkOutlined />
        //                 </Button>
        //             </Tooltip>
        //         );
        //     },
        // },
        {
            dataIndex: 'assign',
            key: 'assign',
            render: (_: any, record: any) => {
                return (
                    <Tooltip title={`Assign ${record.name} role to users`}>
                        <Button
                            onClick={() => {
                                setIsBatchAddRolesModalVisible(true);
                                setActiveRoleInModal(record.urn);
                            }}
                        >
                            ASSIGN
                        </Button>
                    </Tooltip>
                );
            },
        },
    ];

    const tableData = roles?.map((role) => ({
        urn: role?.urn,
        type: role?.type,
        description: role?.description,
        name: role?.name,
        users: role?.relationships?.relationships.map((relationship) => relationship.entity as CorpUser),
    }));

    return (
        <PageContainer>
            {rolesLoading && !rolesData && (
                <Message type="loading" content="Loading roles..." style={{ marginTop: '10%' }} />
            )}
            {rolesError && message.error('Failed to load roles :(')}
            <SourceContainer>
                <TabToolbar>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search roles..."
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
                            titleText={`Assign ${activeRoleInModal} Role to Users`}
                            continueText="Add"
                            onContinue={batchAssignRoleToActors}
                            onCancel={() => {
                                setIsBatchAddRolesModalVisible(false);
                                setActiveRoleInModal('');
                            }}
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
                        emptyText: <Empty description="No Policies!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
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
        </PageContainer>
    );
};
