import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { PostList } from '@app/settingsV2/posts/PostsList';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

export default function ManagePosts() {
    return (
        <PageContainer data-testid="managePostsV2">
            <PageTitle
                title="Home Page"
                subTitle="View and manage pinned announcements and links that appear to all users on the landing page."
            />
            <ListContainer>
                <PostList />
            </ListContainer>
        </PageContainer>
    );
}
