import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ViewsList } from './ViewsList';

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

/**
 * Component used for displaying the 'Manage Views' experience.
 */
export const ManageViews = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Manage Views</PageTitle>
                <Typography.Paragraph type="secondary">
                    Create, edit, and remove your Views. Views allow you to save and share sets of filters for reuse
                    when browsing DataHub.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
