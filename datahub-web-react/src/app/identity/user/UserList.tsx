import React, { useEffect, useState } from 'react';
import { Button, Empty, List, Pagination } from 'antd';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { UsergroupAddOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router';
import UserListItem from './UserListItem';
import { Message } from '../../shared/Message';
import { useListUsersQuery } from '../../../graphql/user.generated';
import { CorpUser, DataHubRole } from '../../../types.generated';
import TabToolbar from '../../entity/shared/components/styled/TabToolbar';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import ViewInviteTokenModal from './ViewInviteTokenModal';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import { useListRolesQuery } from '../../../graphql/role.generated';
import { scrollToTop } from '../../shared/searchUtils';

const UserContainer = styled.div``;

const UserStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const UserPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

export const UserList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [isViewingInviteToken, setIsViewingInviteToken] = useState(false);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const authenticatedUser = useGetAuthenticatedUser();
    const canManageUserCredentials = authenticatedUser?.platformPrivileges.manageUserCredentials || false;

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading: usersLoading,
        error: usersError,
        data: usersData,
        refetch: usersRefetch,
    } = useListUsersQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalUsers = usersData?.listUsers?.total || 0;
    const users = usersData?.listUsers?.users || [];
    const filteredUsers = users.filter((user) => !removedUrns.includes(user.urn));

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        // Hack to deal with eventual consistency.
        const newRemovedUrns = [...removedUrns, urn];
        setRemovedUrns(newRemovedUrns);
        setTimeout(function () {
            usersRefetch?.();
        }, 3000);
    };

    const {
        loading: rolesLoading,
        error: rolesError,
        data: rolesData,
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

    const loading = usersLoading || rolesLoading;
    const error = usersError || rolesError;
    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    return (
        <>
            {!usersData && loading && <Message type="loading" content="Loading users..." />}
            {error && <Message type="error" content="Failed to load users! An unexpected error occurred." />}
            <UserContainer>
                <TabToolbar>
                    <div>
                        <Button
                            disabled={!canManageUserCredentials}
                            type="text"
                            onClick={() => setIsViewingInviteToken(true)}
                        >
                            <UsergroupAddOutlined /> Invite Users
                        </Button>
                    </div>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search users..."
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
                        hideRecommendations
                    />
                </TabToolbar>
                <UserStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Users!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={filteredUsers}
                    renderItem={(item: any) => (
                        <UserListItem
                            onDelete={() => handleDelete(item.urn as string)}
                            user={item as CorpUser}
                            canManageUserCredentials={canManageUserCredentials}
                            selectRoleOptions={selectRoleOptions}
                            refetch={usersRefetch}
                        />
                    )}
                />
                <UserPaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalUsers}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </UserPaginationContainer>
                {canManageUserCredentials && (
                    <ViewInviteTokenModal
                        visible={isViewingInviteToken}
                        onClose={() => setIsViewingInviteToken(false)}
                    />
                )}
            </UserContainer>
        </>
    );
};
