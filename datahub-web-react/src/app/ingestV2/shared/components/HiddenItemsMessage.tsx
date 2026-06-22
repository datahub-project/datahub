import { Icon, Text } from '@components';
import { Lock } from '@phosphor-icons/react/dist/csr/Lock';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    justify-content: center;
    align-items: center;
    padding: 8px;
`;

interface Props {
    message?: string;
}

export default function HiddenItemsMessage({ message }: Props) {
    const { t } = useTranslation('ingestion');
    return (
        <Container>
            <Icon icon={Lock} size="lg" />{' '}
            <Text weight="bold" size="sm">
                {message}
            </Text>{' '}
            <Text size="sm">{t('source.contactAdminForAccess')}</Text>
        </Container>
    );
}
