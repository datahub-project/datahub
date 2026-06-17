import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
    text-align: center;
    colors: ${(props) => props.theme.colors.textSecondary};
`;

export const DocumentTreeEmptyState: React.FC = () => {
    const { t } = useTranslation('home.v2');
    return <EmptyStateContainer>{t('documents.emptyState')}</EmptyStateContainer>;
};
