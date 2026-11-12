import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Text = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

export const EmptyGroupsYouAreIn = () => {
    const { t } = useTranslation('home.v2');
    return <Text>{t('yourGroups.empty')}</Text>;
};
