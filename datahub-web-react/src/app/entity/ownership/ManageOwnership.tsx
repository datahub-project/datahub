import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation();
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>{t('settings.manageWithName', { name: t('common.propriety') })}</PageTitle>
                <Typography.Paragraph type="secondary">
                    {t('settings.manageOwnershipTypeDescription')}
                </Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <OwnershipList />
            </ListContainer>
        </PageContainer>
    );
};
