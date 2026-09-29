import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const SecondaryText = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
`;

/**
 * No results yet summarization.
 */
export const NoResultsSummary = () => {
    const { t } = useTranslation('entity.profile.validations');
    return <SecondaryText>{t('noResults.thisAssertionNotEvaluated')}</SecondaryText>;
};
