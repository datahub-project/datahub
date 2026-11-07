import { EmptyState } from '@components';
import { LockSimple } from '@phosphor-icons/react/dist/csr/LockSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Container = styled.div`
    padding: 48px 32px;
`;

export const UnauthorizedPage = () => {
    const { t } = useTranslation('misc');
    return (
        <Container>
            <EmptyState
                icon={LockSimple}
                size="lg"
                title={t('authorization.unauthorizedTitle')}
                description={t('authorization.unauthorizedSubtitle')}
            />
        </Container>
    );
};
