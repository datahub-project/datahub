import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ViewsList } from './ViewsList';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
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

/**
 * Component used for displaying the 'Manage Views' experience.
 */
export const ManageViews = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>管理视图</PageTitle>
                <Typography.Paragraph type="secondary">
                    创建，编辑，删除您的视图。 视图是一组过滤条件的集合，可以在Datahub中重复使用并分享.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
