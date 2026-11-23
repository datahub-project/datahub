import { Card, Icon } from '@components';
import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { XCircle } from '@phosphor-icons/react/dist/csr/XCircle';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const CardsRow = styled.div`
    display: flex;
    flex-shrink: 0;
    gap: 12px;
    padding: 20px;

    & > * {
        flex: 1;
        min-width: 0;
    }
`;

const IconContainer = styled.div<{ $variant: 'success' | 'error' }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: 8px;
    background: ${(props) =>
        props.$variant === 'success' ? props.theme.colors.bgSurfaceSuccess : props.theme.colors.bgSurfaceError};
`;

type Props = {
    passing: number;
    failing: number;
};

export const TestResultsSummary = ({ passing, failing }: Props) => {
    const { t } = useTranslation('entity.profile.tests');

    return (
        <CardsRow>
            <Card
                title={String(passing)}
                subTitle={t('summary.passingLabel')}
                width="100%"
                icon={
                    <IconContainer $variant="success">
                        <Icon icon={CheckCircle} color="iconSuccess" size="2xl" />
                    </IconContainer>
                }
            />
            <Card
                title={String(failing)}
                subTitle={t('summary.failingLabel')}
                width="100%"
                icon={
                    <IconContainer $variant="error">
                        <Icon icon={XCircle} color="iconError" size="2xl" />
                    </IconContainer>
                }
            />
        </CardsRow>
    );
};
