import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DomainsList } from './DomainsList';

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

export const ManageDomainsPage = () => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>Domains</PageTitle>
                <Typography.Paragraph type="secondary">
                    View your DataHub Domains. Take administrative actions.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <DomainsList />
            </ListContainer>
        </PageContainer>
    );
};
