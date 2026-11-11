import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Text } from '@components/components/Text';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 8px 0;
`;

type Props = {
    message?: string;
    /** Rendered inline right after the message, e.g. a suggestion with a link. */
    children?: React.ReactNode;
};

export function NoResultsFound({ message, children }: Props) {
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');

    return (
        <Container data-testid="no-results-found">
            <Text size="md">{tc('noResults')}</Text>
            <Text size="sm">
                {message ?? t('select.noResults.description')}
                {children}
            </Text>
        </Container>
    );
}
