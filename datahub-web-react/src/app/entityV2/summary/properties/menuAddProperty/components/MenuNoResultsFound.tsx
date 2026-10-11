import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const NoResultsFoundWrapper = styled.div`
    text-align: center;
    padding: 4px;
`;

export default function MenuNoResultsFound() {
    const { t: tc } = useTranslation('common.actions');

    return (
        <NoResultsFoundWrapper>
            <Text>{tc('noResults')}</Text>
        </NoResultsFoundWrapper>
    );
}
