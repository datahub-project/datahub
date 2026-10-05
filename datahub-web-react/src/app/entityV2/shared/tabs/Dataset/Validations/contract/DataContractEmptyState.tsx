import { Button, Text } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Summary = styled.div`
    width: 100%;
    padding: 20px 32px 20px 40px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
`;

const SummaryMessage = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-width: 350px;
`;

type Props = {
    showContractBuilder: () => void;
};

export const DataContractEmptyState = ({ showContractBuilder }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    return (
        <Summary>
            <SummaryMessage>
                <Text size="lg" weight="bold">
                    {t('dataContractEmptyState.title')}
                </Text>
                <Text color="textSecondary">{t('dataContractEmptyState.description')}</Text>
            </SummaryMessage>
            <Button icon={{ icon: Plus }} onClick={showContractBuilder} data-testid="create-contract-button">
                {tc('create')}
            </Button>
        </Summary>
    );
};
