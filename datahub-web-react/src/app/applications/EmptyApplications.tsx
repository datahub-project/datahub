import { EmptyState } from '@components';
import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

type Props = {
    isEmptySearch: boolean;
};

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px;
`;

const EmptyApplications = ({ isEmptySearch }: Props) => {
    const { t } = useTranslation('misc');
    return (
        <EmptyContainer data-testid="applications-not-found">
            <EmptyState
                icon={AppWindow}
                title={isEmptySearch ? t('applications.emptySearch') : t('applications.empty')}
                description={isEmptySearch ? undefined : t('applications.emptyDescription')}
            />
        </EmptyContainer>
    );
};

export default EmptyApplications;
