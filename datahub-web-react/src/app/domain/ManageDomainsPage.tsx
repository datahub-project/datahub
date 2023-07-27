import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Trans } from 'react-i18next';
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
                <PageTitle level={3}> <Trans>Domains</Trans></PageTitle>
                <Typography.Paragraph type="secondary">
                    <Trans>View your DataHub Domains</Trans>. <Trans>Take administrative actions</Trans>.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <DomainsList />
            </ListContainer>
        </PageContainer>
    );
};
