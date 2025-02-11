import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { OwnershipList } from './OwnershipList';

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
 * Component used for displaying the 'Manage Ownership' experience.
 */
export const ManageOwnership = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Manage Ownership</PageTitle>
                <Typography.Paragraph type="secondary">
                    Create, edit, and remove custom Ownership Types.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <OwnershipList />
            </ListContainer>
        </PageContainer>
    );
};
