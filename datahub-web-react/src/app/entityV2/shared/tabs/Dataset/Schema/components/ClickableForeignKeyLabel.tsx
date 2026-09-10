import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ForeignKeyPillButton = styled.button`
    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.borderSuccess};
    border-radius: 10px;
    color: ${(props) => props.theme.colors.textSuccess};
    cursor: pointer;
    font-size: 12px;
    font-weight: 400;
    line-height: inherit;
    padding: 0 8px;
`;

interface Props {
    onClick: () => void;
}

export default function ClickableForeignKeyLabel({ onClick }: Props) {
    const { t } = useTranslation('entity.profile.schema');

    return (
        <ForeignKeyPillButton
            type="button"
            onClick={(event) => {
                event.stopPropagation();
                onClick();
            }}
        >
            {t('constraintLabels.foreignKey')}
        </ForeignKeyPillButton>
    );
}
