import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation();
    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>{t('entity.manageView')}</PageTitle>
                <Typography.Paragraph type="secondary">{t('filter.view.manageViewDescription')}</Typography.Paragraph>
            </PageHeaderContainer>
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
