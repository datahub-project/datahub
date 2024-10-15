import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { PostList } from './PostsList';

export const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

export const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

export const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

export default function ManagePosts() {
    return (
        <PageContainer>
            <PageHeaderContainer data-testid="managePostsV2">
                <PageTitle level={3}>Home Page</PageTitle>
                <Typography.Paragraph type="secondary">
                    View and manage pinned announcements and links that appear to all users on the landing page.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <PostList />
            </ListContainer>
        </PageContainer>
    );
}
