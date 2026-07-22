import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const OptionalPromptsWrapper = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    margin-top: 4px;
`;

interface Props {
    numRemaining: number;
}

export default function OptionalPromptsRemaining({ numRemaining }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    if (numRemaining <= 0) return null;

    return (
        <OptionalPromptsWrapper>
            {t('formInfo.optionalQuestionsRemainingCount', { count: numRemaining })}
        </OptionalPromptsWrapper>
    );
}
