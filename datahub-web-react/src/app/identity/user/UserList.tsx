import React, { useState } from 'react';
import { Empty, List, message, Pagination } from 'antd';
import styled from 'styled-components';
import UserListItem from './UserListItem';
import { Message } from '../../shared/Message';
import { useListUsersQuery } from '../../../graphql/user.generated';
import { CorpUser } from '../../../types.generated';

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
    const [page, setPage] = useState(1);
    const [removedUrns, setRemovedUrns] = useState<string[]>([]);

    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListUsersQuery({
        variables: {
            input: {
                start,
                count: pageSize,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalUsers = data?.listUsers?.total || 0;
    const users = data?.listUsers?.users || [];
    const filteredUsers = users.filter((user) => !removedUrns.includes(user.urn));

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        // Hack to deal with eventual consistency.
        const newRemovedUrns = [...removedUrns, urn];
        setRemovedUrns(newRemovedUrns);
        setTimeout(function () {
            refetch?.();
        }, 3000);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading users..." />}
            {error && message.error('Failed to load users :(')}
            <UserContainer>
                <UserStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Users!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={filteredUsers}
                    renderItem={(item: any) => (
                        <UserListItem onDelete={() => handleDelete(item.urn as string)} user={item as CorpUser} />
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
            </UserContainer>
        </>
    );
};
