/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { PostList } from '@app/settings/posts/PostsList';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
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

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

export default function ManagePosts() {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Home Page Posts</PageTitle>
                <Typography.Paragraph type="secondary">
                    View and manage pinned posts that appear to all users on the landing page.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <PostList />
            </ListContainer>
        </PageContainer>
    );
}
