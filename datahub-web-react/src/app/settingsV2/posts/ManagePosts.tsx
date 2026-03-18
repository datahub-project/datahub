import { Button, PageTitle } from '@components';
import React, { useState } from 'react';
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

const PageHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

export default function ManagePosts() {
    const [isCreatingPost, setIsCreatingPost] = useState(false);

    return (
        <PageContainer data-testid="managePostsV2">
            <PageHeader>
                <PageTitle
                    title="Home Page"
                    subTitle="View and manage pinned announcements and links that appear to all users on the landing page."
                />
                <Button
                    variant="filled"
                    data-testid="posts-create-post-v2"
                    icon={{ icon: 'Plus', source: 'phosphor' }}
                    onClick={() => setIsCreatingPost(true)}
                >
                    Create new post
                </Button>
            </PageHeader>
            <ListContainer>
                <PostList isCreatingPost={isCreatingPost} setIsCreatingPost={setIsCreatingPost} />
            </ListContainer>
        </PageContainer>
    );
}
