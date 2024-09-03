import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { useTranslation } from 'react-i18next';
import { PostList } from './PostsList';

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
    const { t } = useTranslation();
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>{t('settings.homePagePosts')}</PageTitle>
                <Typography.Paragraph type="secondary">{t('settings.homePagePostsDescription')}</Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <PostList />
            </ListContainer>
        </PageContainer>
    );
}
