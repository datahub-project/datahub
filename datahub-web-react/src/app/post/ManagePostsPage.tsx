import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { PostList } from './PostsList';

const PageContainer = styled.div`
    padding-top: 20px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const ListContainer = styled.div``;

export const ManagePostPage = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Homepage Posts</PageTitle>
                <Typography.Paragraph type="secondary">View and manage your DataHub Posts.</Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <PostList />
            </ListContainer>
        </PageContainer>
    );
};
